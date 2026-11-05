// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"context"
	"math"
	"slices"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/sets"
	ksf "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/k8s_internal"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/scheduler_util"
)

// gpuTypeLabel is only used to describe excluded nodes in the temporary log line.
const gpuTypeLabel = "cloud.google.com/gke-accelerator"

// FilterVictimsToUsableNodes removes from candidates the victim jobs that have no alive task on a
// node the preemptor could use (see NodesUsableByJob): evicting them frees nothing the preemptor
// can use. It returns what it removed.
//
// NodesUsableByJob returns nil when the preemptor places no restriction on nodes: every candidate
// is kept. An empty non-nil set means no node is usable: every candidate is excluded.
func FilterVictimsToUsableNodes(
	ssn *framework.Session, preemptor *podgroup_info.PodGroupInfo,
	candidates map[common_info.PodGroupID]*podgroup_info.PodGroupInfo, action framework.ActionType,
) VictimFilterStats {
	stats := VictimFilterStats{CandidateJobs: len(candidates), UsableNodes: len(ssn.ClusterInfo.Nodes)}
	if len(candidates) == 0 {
		stats.log(ssn, preemptor, action)
		return stats
	}
	usable := NodesUsableByJob(ssn, preemptor, candidates)
	if usable != nil {
		stats.UsableNodes = len(usable)
		for id, job := range candidates {
			if !JobHasTaskOnNodes(job, usable) {
				delete(candidates, id)
				stats.countExcluded(job)
			} else if trimmed := withoutTasksOffNodes(job, usable); trimmed != nil {
				candidates[id] = trimmed
				stats.ExcludedPods += aliveTasks(job) - len(trimmed.GetAllPodsMap())
			}
		}
	}
	stats.log(ssn, preemptor, action)
	return stats
}

// NodesUsableByJob returns the names of the nodes on which at least one of the job's tasks to
// allocate could run once the victims are evicted. A node qualifies for a task when:
//   - it passes the predicates that do not depend on the pods on the node: node selector and
//     required node affinity, taints and tolerations, and node conditions (including cordon);
//   - for every required topology constraint above the task, the node's domain could hold the
//     constrained group if every victim in it were evicted (see viableDomains).
//
// It returns nil, meaning no restriction, when the job has no tasks to allocate.
func NodesUsableByJob(
	ssn *framework.Session, job *podgroup_info.PodGroupInfo,
	victims map[common_info.PodGroupID]*podgroup_info.PodGroupInfo,
) sets.Set[string] {
	tasks := podgroup_info.GetTasksToAllocate(
		job, ssn.SubGroupOrderFn, ssn.TaskOrderFn, podgroup_info.SimulatedTaskAllocation)
	if len(tasks) == 0 {
		return nil
	}

	filters := nodeFilters(ssn)
	// The filters only read the cycle state (NodeAffinity falls back to the pod when it holds no
	// prefilter state, TaintToleration ignores it), so one state serves every pair.
	state := k8s_internal.NewSessionState()
	k8sNodes := map[string]*k8sframework.NodeInfo{}
	// Tasks with the same node constraints have the same statically usable nodes.
	var representatives []*pod_info.PodInfo
	var staticNodes []sets.Set[string]
	staticNodesByTask := map[*pod_info.PodInfo]sets.Set[string]{}
	staticNodesOf := func(task *pod_info.PodInfo) sets.Set[string] {
		if nodes, found := staticNodesByTask[task]; found {
			return nodes
		}
		for i, rep := range representatives {
			if sameNodeConstraints(task.Pod, rep.Pod) {
				staticNodesByTask[task] = staticNodes[i]
				return staticNodes[i]
			}
		}
		nodes := sets.New[string]()
		for name, node := range ssn.ClusterInfo.Nodes {
			if taskCanUseNode(task, node, k8sNodes, state, filters) {
				nodes.Insert(name)
			}
		}
		representatives = append(representatives, task)
		staticNodes = append(staticNodes, nodes)
		staticNodesByTask[task] = nodes
		return nodes
	}

	groupsByTask := map[*pod_info.PodInfo][]subgroup_info.SubGroupMember{}
	tasksByGroup := map[subgroup_info.SubGroupMember][]*pod_info.PodInfo{}
	for _, task := range tasks {
		groups := requiredTopologyGroups(job, task)
		groupsByTask[task] = groups
		for _, group := range groups {
			tasksByGroup[group] = append(tasksByGroup[group], task)
		}
	}
	var domains map[subgroup_info.SubGroupMember]sets.Set[string]
	if len(tasksByGroup) > 0 {
		domains = map[subgroup_info.SubGroupMember]sets.Set[string]{}
		for group, groupTasks := range tasksByGroup {
			nodes := sets.New[string]()
			for _, task := range groupTasks {
				for name := range staticNodesOf(task) {
					nodes.Insert(name)
				}
			}
			if viable, ok := viableDomains(ssn, group, groupTasks, nodes, victims); ok {
				domains[group] = viable
			}
		}
	}

	usable := sets.New[string]()
	for _, task := range tasks {
		groups := groupsByTask[task]
		for name := range staticNodesOf(task) {
			if inViableDomains(ssn.ClusterInfo.Nodes[name], groups, domains) {
				usable.Insert(name)
			}
		}
	}
	return usable
}

// withoutTasksOffNodes returns a copy of an elastic victim job without its tasks on other nodes,
// when the job keeps its minimum on those nodes even if every task on the given nodes is
// evicted. Otherwise, or when nothing runs on other nodes, it returns nil and the job stays a
// victim as a whole, as KAI evicts gangs.
func withoutTasksOffNodes(job *podgroup_info.PodGroupInfo, nodes sets.Set[string]) *podgroup_info.PodGroupInfo {
	if len(job.PodSets) != 1 || job.IsSemiPreemptibleJob() {
		return nil
	}
	var onNodes []*pod_info.PodInfo
	offNodes := 0
	for _, task := range job.GetAllPodsMap() {
		if !pod_status.IsAliveStatus(task.Status) {
			continue
		}
		if nodes.Has(task.NodeName) {
			onNodes = append(onNodes, task)
		} else {
			offNodes++
		}
	}
	var podSet *subgroup_info.PodSet
	for _, ps := range job.PodSets {
		podSet = ps
	}
	if offNodes == 0 || offNodes < int(podSet.GetMinAvailable()) {
		return nil
	}
	trimmed := job.CloneWithTasks(onNodes)
	for _, ps := range trimmed.PodSets {
		ps.SetMinAvailable(0)
	}
	return trimmed
}

// JobHasTaskOnNodes reports whether any alive task of the job runs on one of the nodes. A nil set
// means every node.
func JobHasTaskOnNodes(job *podgroup_info.PodGroupInfo, nodes sets.Set[string]) bool {
	if nodes == nil {
		return true
	}
	for _, task := range job.GetAllPodsMap() {
		if pod_status.IsAliveStatus(task.Status) && nodes.Has(task.NodeName) {
			return true
		}
	}
	return false
}

func nodeFilters(ssn *framework.Session) []k8s_internal.NodeFilter {
	// Sessions built without a cache (some unit tests) have no k8s plugins: only the node
	// condition check applies then.
	if ssn.Cache == nil || ssn.InternalK8sPlugins() == nil {
		return nil
	}
	plugins := ssn.InternalK8sPlugins()
	var filters []k8s_internal.NodeFilter
	for _, plugin := range []ksf.Plugin{plugins.NodeAffinity, plugins.TaintToleration} {
		if filter, ok := plugin.(k8s_internal.NodeFilter); ok {
			filters = append(filters, filter)
		}
	}
	return filters
}

// taskCanUseNode runs the pod-independent predicates. k8sNodes caches the framework node of each
// node name across calls.
func taskCanUseNode(
	task *pod_info.PodInfo, node *node_info.NodeInfo, k8sNodes map[string]*k8sframework.NodeInfo,
	state k8s_internal.SessionState, filters []k8s_internal.NodeFilter,
) bool {
	if task.Pod == nil || node == nil || node.Node == nil {
		return true
	}
	if fit, _, _ := scheduler_util.CheckNodeConditionPredicate(node.Node, task.Pod.Spec.Tolerations); !fit {
		return false
	}
	nodeInfo, found := k8sNodes[node.Name]
	if !found {
		nodeInfo = k8sframework.NewNodeInfo()
		nodeInfo.SetNode(node.Node)
		k8sNodes[node.Name] = nodeInfo
	}
	for _, filter := range filters {
		if status := filter.Filter(context.Background(), state, task.Pod, nodeInfo); !status.IsSuccess() {
			return false
		}
	}
	return true
}

func sameNodeConstraints(a, b *v1.Pod) bool {
	if a == nil || b == nil {
		return a == b
	}
	return equality.Semantic.DeepEqual(a.Spec.NodeSelector, b.Spec.NodeSelector) &&
		equality.Semantic.DeepEqual(nodeAffinity(a), nodeAffinity(b)) &&
		equality.Semantic.DeepEqual(a.Spec.Tolerations, b.Spec.Tolerations)
}

func nodeAffinity(pod *v1.Pod) *v1.NodeAffinity {
	if pod.Spec.Affinity == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity
}

// requiredTopologyGroups returns the task's pod set and its ancestors that carry a required
// topology level.
func requiredTopologyGroups(job *podgroup_info.PodGroupInfo, task *pod_info.PodInfo) []subgroup_info.SubGroupMember {
	name := task.SubGroupName
	if name == "" {
		name = podgroup_info.DefaultSubGroup
	}
	podSet, found := job.PodSets[name]
	if !found {
		return nil
	}
	var groups []subgroup_info.SubGroupMember
	if hasRequiredLevel(podSet) {
		groups = append(groups, podSet)
	}
	for parent := podSet.GetParent(); parent != nil; parent = parent.GetParent() {
		if hasRequiredLevel(parent) {
			groups = append(groups, parent)
		}
	}
	return groups
}

func hasRequiredLevel(group subgroup_info.SubGroupMember) bool {
	constraint := group.GetTopologyConstraint()
	return constraint != nil && constraint.RequiredLevel != ""
}

// viableDomains returns the values of the group's required topology label for which the domain,
// with every victim evicted, has enough GPUs for the group's minimum pods. It errs towards
// keeping a domain: it counts only GPUs, treats any GPU not used by a non-victim pod as free, and
// merges domains that share a label value. ok is false when the check does not apply (the group
// already has running pods, a non-whole-GPU task, or a sub-group set that needs only some of its
// members), and then the group restricts nothing.
func viableDomains(
	ssn *framework.Session, group subgroup_info.SubGroupMember, tasks []*pod_info.PodInfo,
	nodes sets.Set[string], victims map[common_info.PodGroupID]*podgroup_info.PodGroupInfo,
) (sets.Set[string], bool) {
	podSets := descendantPodSets(group)
	if podSets == nil {
		return nil, false
	}
	gpusPerPod := math.NaN()
	for _, task := range tasks {
		gpus := task.GpuRequirement.GPUs()
		if !task.IsRegularGPURequest() || task.GpuRequirement.IsFractionalRequest() || gpus < 1 ||
			len(task.GpuRequirement.MigResources()) > 0 || task.GpuRequirement.GetDraGpusCount() > 0 {
			return nil, false
		}
		if math.IsNaN(gpusPerPod) {
			gpusPerPod = gpus
		} else if gpus != gpusPerPod {
			return nil, false
		}
	}
	podsNeeded := 0
	for _, podSet := range podSets {
		if podSet.GetNumActiveAllocatedTasks() > 0 {
			return nil, false
		}
		podsNeeded += int(podSet.GetMinAvailable())
	}

	label := group.GetTopologyConstraint().RequiredLevel
	podsByDomain := map[string]int{}
	for name := range nodes {
		node := ssn.ClusterInfo.Nodes[name]
		if node == nil || node.Node == nil {
			continue
		}
		domain, found := node.Node.Labels[label]
		if !found {
			continue
		}
		podsByDomain[domain] += podsThatFit(node, victims, gpusPerPod)
	}
	viable := sets.New[string]()
	for domain, pods := range podsByDomain {
		if pods >= podsNeeded {
			viable.Insert(domain)
		}
	}
	return viable, true
}

// descendantPodSets returns the pod sets under the group, or nil if a sub-group set in it needs
// only some of its members.
func descendantPodSets(group subgroup_info.SubGroupMember) []*subgroup_info.PodSet {
	switch g := group.(type) {
	case *subgroup_info.PodSet:
		return []*subgroup_info.PodSet{g}
	case *subgroup_info.SubGroupSet:
		if minSubGroup := g.GetMinSubGroup(); minSubGroup != nil &&
			int(*minSubGroup) < len(g.GetDirectSubgroupsSets())+len(g.GetDirectPodSets()) {
			return nil
		}
		var podSets []*subgroup_info.PodSet
		for _, podSet := range g.GetDirectPodSets() {
			podSets = append(podSets, podSet)
		}
		for _, child := range g.GetDirectSubgroupsSets() {
			childPodSets := descendantPodSets(child)
			if childPodSets == nil {
				return nil
			}
			podSets = append(podSets, childPodSets...)
		}
		return podSets
	}
	return nil
}

// gpuEpsilon absorbs the float error of freeableGPUs, which subtracts fractional GPU quotas: a
// node with 7 free GPUs must not come out as 6.999999999 and truncate to 6 pods.
const gpuEpsilon = 1e-6

// podsThatFit is the number of pods of gpusPerPod whole GPUs the node holds once every victim is
// evicted.
func podsThatFit(node *node_info.NodeInfo, victims map[common_info.PodGroupID]*podgroup_info.PodGroupInfo, gpusPerPod float64) int {
	return int((freeableGPUs(node, victims) + gpuEpsilon) / gpusPerPod)
}

// freeableGPUs is the node's GPUs minus those used by pods that are not victims.
func freeableGPUs(node *node_info.NodeInfo, victims map[common_info.PodGroupID]*podgroup_info.PodGroupInfo) float64 {
	free := node.AllocatableVector.TotalGPUs(node.VectorMap)
	for _, task := range node.PodInfos {
		if !pod_status.IsActiveUsedStatus(task.Status) {
			continue
		}
		if _, isVictim := victims[task.Job]; isVictim {
			continue
		}
		free -= task.GpuRequirement.GetGpusQuota()
	}
	return free
}

func inViableDomains(
	node *node_info.NodeInfo, groups []subgroup_info.SubGroupMember,
	domains map[subgroup_info.SubGroupMember]sets.Set[string],
) bool {
	for _, group := range groups {
		viable, checked := domains[group]
		if !checked || node == nil || node.Node == nil {
			continue
		}
		domain, found := node.Node.Labels[group.GetTopologyConstraint().RequiredLevel]
		if found && !viable.Has(domain) {
			return false
		}
	}
	return true
}

// VictimFilterStats describes one run of FilterVictimsToUsableNodes.
type VictimFilterStats struct {
	CandidateJobs int
	UsableNodes   int
	ExcludedJobs  int
	ExcludedPods  int
	// ExcludedNodes holds the 3 first node names, in order, of excluded pods.
	ExcludedNodes []string
}

func (s *VictimFilterStats) countExcluded(job *podgroup_info.PodGroupInfo) {
	s.ExcludedJobs++
	for _, task := range job.GetAllPodsMap() {
		if !pod_status.IsAliveStatus(task.Status) {
			continue
		}
		s.ExcludedPods++
		if task.NodeName != "" && !slices.Contains(s.ExcludedNodes, task.NodeName) {
			s.ExcludedNodes = append(s.ExcludedNodes, task.NodeName)
			slices.Sort(s.ExcludedNodes)
			if len(s.ExcludedNodes) > 3 {
				s.ExcludedNodes = s.ExcludedNodes[:3]
			}
		}
	}
}

func (s *VictimFilterStats) log(ssn *framework.Session, preemptor *podgroup_info.PodGroupInfo, action framework.ActionType) {
	// Only decisions that change the victim set are logged: every pending job tries to preempt and
	// reclaim each session, so a line per attempt would flood the scheduler log.
	if s.ExcludedJobs == 0 {
		return
	}
	sample := make([]string, 0, len(s.ExcludedNodes))
	for _, name := range s.ExcludedNodes {
		gpuType := "none"
		if node := ssn.ClusterInfo.Nodes[name]; node != nil && node.Node != nil {
			if value, found := node.Node.Labels[gpuTypeLabel]; found {
				gpuType = value
			}
		}
		sample = append(sample, name+"("+gpuType+")")
	}
	// TODO(II-2486): remove after rollout verification
	log.InfraLogger.Infof("[victim-filter] preemptor=%s/%s action=%s nodes=%d usable_nodes=%d "+
		"victim_jobs=%d excluded_jobs=%d excluded_pods=%d excluded_nodes_sample=%v",
		preemptor.Namespace, preemptor.Name, action, len(ssn.ClusterInfo.Nodes), s.UsableNodes,
		s.CandidateJobs, s.ExcludedJobs, s.ExcludedPods, sample)
}

func aliveTasks(job *podgroup_info.PodGroupInfo) int {
	alive := 0
	for _, task := range job.GetAllPodsMap() {
		if pod_status.IsAliveStatus(task.Status) {
			alive++
		}
	}
	return alive
}
