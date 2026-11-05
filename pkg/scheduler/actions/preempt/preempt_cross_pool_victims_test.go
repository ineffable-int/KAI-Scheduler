// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt_test

import (
	"fmt"
	"maps"
	"sort"
	"testing"

	kaiv1alpha1 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/kai/v1alpha1"
	. "go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/eviction_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/topology_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/cache"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

// Reproduction of kai-scheduler/KAI-Scheduler#2426 as seen on voyager-7: a multi-node gang
// that can only run on GB200 nodes (nodeSelector, plus a rack topology constraint) preempts
// and the committed victim set contains pods that run on nodes the gang can never use: RTX
// nodes, or GB200 racks too small for the gang. Those pods are deleted for nothing.
//
// Every case states the evictions it expects per pool and how many preemptor pods get placed.
// Cases marked failsOn41467076 fail on v0.18.5 (fork commit 41467076). Run
//
//	go test ./pkg/scheduler/actions/preempt/ -run TestPreemptGangDoesNotEvictOtherPool -v
//
// The scenario mirrors production: the scheduler runs with allowConsolidatingReclaim=true
// (the test session default) and the voyager-7 plugins (sg-nodelocalgreedy, sg-multinodegang).

const (
	acceleratorLabel = "cloud.google.com/gke-accelerator"
	subblockLabel    = "cloud.google.com/gce-topology-subblock"
	archLabel        = "kubernetes.io/arch"
	gb200            = "nvidia-gb200"
	rtx              = "nvidia-rtx-pro-6000"

	lowPriority    int32 = 25
	mediumPriority int32 = 50
	highPriority   int32 = 75
)

type gangConstraint int

const (
	// byNodeSelector: the GB200 pods of the gang select nodes with gke-accelerator=nvidia-gb200.
	byNodeSelector gangConstraint = iota
	// byNodeAffinity: required node affinity on the GB200 node names.
	byNodeAffinity
	// byTaint: no selector; the RTX nodes carry a NoSchedule taint the gang does not tolerate.
	byTaint
	// byArch: kubernetes.io/arch=arm64 (GB200 nodes are arm64, RTX nodes amd64).
	byArch
)

type crossPoolCase struct {
	name string
	// racks and nodesPerRack describe the GB200 pool (4 GPUs per node).
	racks, nodesPerRack int
	// rtxNodes and rtxGPUs describe the RTX pool. Every RTX node is full with single-GPU jobs
	// (like the leaderboard pods): rtxLowGPUs of them low priority (preemptible), the rest high
	// priority.
	rtxNodes, rtxGPUs, rtxLowGPUs int
	// gangNodes is the number of GB200 pods in the preemptor gang, gangGPUs GPUs each (0: 4).
	gangNodes, gangGPUs int
	// constraint is how the GB200 pods of the gang are kept off RTX nodes.
	constraint gangConstraint
	// withTopology adds the rack topology constraint fly sets on GB200 gangs.
	withTopology bool
	// highPriorityRacks lists racks held by non-preemptible (high priority) jobs.
	highPriorityRacks []int
	// blockedNodes is the number of nodes at the end of each rack (rack index -> count) held by
	// a non-preemptible job, so the rack cannot fit the whole gang.
	blockedNodes map[int]int
	// rackPriority overrides the priority of the preemptible jobs of a rack (default fly-low).
	// KAI tries lower-priority victims first.
	rackPriority map[int]int32
	// rtxCPUOnly makes the RTX jobs CPU-only pods (4 CPUs, no GPU), like the leaderboard pods on
	// voyager-7: the RTX nodes then keep all their GPUs idle.
	rtxCPUOnly bool
	// gbJobNodes is the number of nodes each low-priority GB200 job spans (a gang of that many
	// pods, one per node); 0 means 1.
	gbJobNodes int
	// smallJobsRack0 replaces the jobs of rack 0 with that many single-GPU low-priority jobs.
	smallJobsRack0 int
	// elasticVictim replaces the jobs of the first two nodes of rack 0 with one elastic
	// low-priority job (min 1) of single-GPU pods that also runs 2 pods on rtx-n0.
	elasticVictim bool
	// rtxJobsNewer makes the RTX jobs newer than the GB200 jobs, so they are tried first as
	// victims (KAI orders victims of equal priority newest first).
	rtxJobsNewer bool
	// cordonedRacks are cordoned (spec.unschedulable); tolerateCordon makes the gang tolerate
	// node.kubernetes.io/unschedulable, which fork patch a2764758 honours.
	cordonedRacks  []int
	tolerateCordon bool
	// rtxPodSet adds a second pod set of that many single-GPU pods with an RTX nodeSelector.
	rtxPodSet int
	// cpuPodSet adds a second pod set with one CPU-only pod restricted to arm64 (GB200) nodes.
	cpuPodSet bool
	// rtxPreemptor, when > 0, makes the preemptor a single pod needing that many GPUs on an RTX
	// node instead of a GB200 gang.
	rtxPreemptor int

	// want is the exact number of evictions per pool.
	want map[string]int
	// unplaceable means no preemptor pod can be placed; otherwise all of them must be.
	unplaceable bool
	// failsOn41467076 documents that v0.18.5 evicts pods on unusable nodes in this case.
	failsOn41467076 bool
	// note explains evictions the expectation includes that are not about unusable nodes.
	note string
}

func gb(n int) map[string]int       { return map[string]int{gb200: n} }
func pools(g, r int) map[string]int { return map[string]int{gb200: g, rtx: r} }

func TestPreemptGangDoesNotEvictOtherPool(t *testing.T) {
	base := crossPoolCase{racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
		rtxJobsNewer: true}
	with := func(c crossPoolCase, change func(*crossPoolCase)) crossPoolCase {
		change(&c)
		return c
	}
	cases := []crossPoolCase{
		// (a) GB200 gang vs RTX victims.
		with(base, func(c *crossPoolCase) {
			c.name, c.racks, c.rtxNodes, c.want = "node selector, one rack", 1, 4, gb(4)
		}),
		with(base, func(c *crossPoolCase) { c.name, c.want = "node selector, two racks", gb(4) }),
		with(base, func(c *crossPoolCase) {
			c.name, c.rtxCPUOnly, c.want = "node selector, CPU-only RTX pods", true, gb(4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.withTopology, c.highPriorityRacks, c.want = "rack topology, one rack non-preemptible",
				true, []int{1}, gb(4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.withTopology, c.rtxCPUOnly, c.want = "rack topology, CPU-only RTX pods", true, true, gb(4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.withTopology, c.highPriorityRacks, c.gbJobNodes, c.want =
				"rack topology, 2-node GB200 victim gangs", true, []int{1}, 2, gb(4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.withTopology, c.highPriorityRacks, c.rtxJobsNewer, c.want =
				"rack topology, RTX jobs older than GB200 jobs", true, []int{1}, false, gb(4)
		}),
		// (b) Racks that cannot fit the gang. Greedy node-local search locks onto rack 0 (lower
		// priority victims are tried first), which one non-preemptible node keeps too small.
		with(base, func(c *crossPoolCase) {
			c.name = "rack topology, queue leads into a rack that cannot fit the gang"
			c.withTopology, c.blockedNodes, c.rackPriority = true, map[int]int{0: 1}, map[int]int32{0: 10}
			c.want, c.failsOn41467076 = gb(4), true // 41467076: 7 GB200 + 32 RTX
		}),
		with(base, func(c *crossPoolCase) {
			c.name = "rack topology, CPU-only RTX pods, queue leads into a rack that cannot fit the gang"
			c.withTopology, c.blockedNodes, c.rackPriority = true, map[int]int{0: 1}, map[int]int32{0: 10}
			c.rtxCPUOnly, c.rtxLowGPUs = true, 8
			c.want, c.failsOn41467076 = gb(4), true // 41467076: 7 GB200
		}),
		// (c) Other preemptor constraints.
		with(base, func(c *crossPoolCase) {
			c.name, c.constraint = "required node affinity instead of node selector", byNodeAffinity
			c.withTopology, c.blockedNodes, c.rackPriority = true, map[int]int{0: 1}, map[int]int32{0: 10}
			c.want, c.failsOn41467076 = gb(4), true // 41467076: 7 GB200 + 32 RTX
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.constraint = "RTX nodes tainted instead of node selector", byTaint
			c.withTopology, c.blockedNodes, c.rackPriority = true, map[int]int{0: 1}, map[int]int32{0: 10}
			c.want, c.failsOn41467076 = gb(4), true // 41467076: 7 GB200 + 32 RTX
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.constraint = "kubernetes.io/arch selector", byArch
			c.withTopology, c.blockedNodes, c.rackPriority = true, map[int]int{0: 1}, map[int]int32{0: 10}
			c.want, c.failsOn41467076 = gb(4), true // 41467076: 7 GB200 + 32 RTX
		}),
		with(base, func(c *crossPoolCase) {
			c.name = "cordoned rack, gang tolerates the cordon"
			c.withTopology, c.highPriorityRacks, c.cordonedRacks, c.tolerateCordon = true, []int{0}, []int{1}, true
			c.want = gb(4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name = "cordoned rack, gang does not tolerate the cordon"
			c.withTopology, c.highPriorityRacks, c.cordonedRacks = true, []int{0}, []int{1}
			c.want, c.unplaceable = gb(0), true
		}),
		// (d) Gangs whose pods differ: a node counts if any pod of the gang could use it.
		with(base, func(c *crossPoolCase) {
			c.name, c.rtxPodSet = "pod sets for GB200 and RTX", 2
			c.want = pools(4, 30)
			c.note = "the RTX pod set needs RTX victims; evicting 30 for 2 pods is KAI's separate " +
				"over-eviction (41467076 evicts the same)"
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.cpuPodSet, c.withTopology, c.highPriorityRacks = "CPU-only arm64 pod in a GB200 gang", true,
				true, []int{1}
			c.want, c.failsOn41467076 = gb(4), true // 41467076: 4 GB200 + 32 RTX
		}),
		// (e) No regressions.
		with(base, func(c *crossPoolCase) {
			c.name, c.gangNodes, c.rtxPreemptor = "RTX job preempting RTX victims", 0, 4
			c.want = pools(0, 4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.gangNodes, c.gangGPUs = "single-GPU GB200 preemptor", 1, 1
			c.want = gb(1)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.racks, c.withTopology, c.highPriorityRacks = "exactly one of four racks clearable", 4,
				true, []int{1, 2, 3}
			c.want = gb(4)
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.nodesPerRack, c.gangNodes = "45 small jobs on the only clearable rack", 12, 12
			c.withTopology, c.highPriorityRacks, c.smallJobsRack0 = true, []int{1}, 45
			c.want, c.failsOn41467076 = gb(45), true // 41467076: 45 GB200 + 32 RTX
		}),
		with(base, func(c *crossPoolCase) {
			c.name, c.racks, c.withTopology, c.elasticVictim = "elastic victim spanning both pools", 1, true, true
			c.want, c.failsOn41467076 = gb(10), true // 41467076: 10 GB200 + 2 RTX (its own RTX pods)
		}),
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			victimsByPool, placed, wantPlaced := runCrossPoolScenario(t, tc)
			if tc.unplaceable {
				wantPlaced = 0
			}
			if placed != wantPlaced {
				t.Errorf("preemptor placed %d of %d pods (victims: %v)", placed, wantPlaced, victimsByPool)
			}
			want := maps.Clone(tc.want)
			for pool, n := range want {
				if n == 0 {
					delete(want, pool)
				}
			}
			if !maps.Equal(victimsByPool, want) {
				t.Errorf("evictions per pool = %v, want %v %s", victimsByPool, want, tc.note)
			}
		})
	}
}

// runCrossPoolScenario runs preempt and returns the evictions per pool, the number of preemptor
// pods placed and the number of preemptor pods.
func runCrossPoolScenario(t *testing.T, tc crossPoolCase) (map[string]int, int, int) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	nodes := map[string]nodes_fake.TestNodeBasic{}
	var jobs []*jobs_fake.TestJobBasic
	poolOf := map[string]string{}
	gbAge, rtxAge := 1000, 1000
	if tc.rtxJobsNewer {
		rtxAge = 100
	}
	isHigh := map[int]bool{}
	for _, r := range tc.highPriorityRacks {
		isHigh[r] = true
	}
	gbJobNodes := max(tc.gbJobNodes, 1)
	for r := 0; r < tc.racks; r++ {
		var tasks []*tasks_fake.TestTaskBasic
		for i := 0; i < tc.nodesPerRack; i++ {
			name := fmt.Sprintf("gb200-r%d-n%d", r, i)
			nodes[name] = nodes_fake.TestNodeBasic{GPUs: 4, Labels: map[string]string{
				acceleratorLabel: gb200, subblockLabel: fmt.Sprintf("rack%d", r), archLabel: "arm64"}}
			poolOf[name] = gb200
			if r == 0 && (tc.smallJobsRack0 > 0 || tc.elasticVictim && i < 2) {
				continue
			}
			prio := lowPriority
			if p, ok := tc.rackPriority[r]; ok {
				prio = p
			}
			if isHigh[r] || i >= tc.nodesPerRack-tc.blockedNodes[r] {
				prio = highPriority
			}
			tasks = append(tasks, &tasks_fake.TestTaskBasic{NodeName: name, State: pod_status.Running})
			if len(tasks) == gbJobNodes || i == tc.nodesPerRack-1 {
				gbAge--
				jobs = append(jobs, &jobs_fake.TestJobBasic{
					Name: fmt.Sprintf("gb200-job-r%d-n%d", r, i), RequiredGPUsPerTask: 4, Priority: prio,
					QueueName: "queue0", JobAgeInMinutes: gbAge, Tasks: tasks,
				})
				tasks = nil
			}
		}
	}
	for j := 0; j < tc.smallJobsRack0; j++ {
		jobs = append(jobs, &jobs_fake.TestJobBasic{
			Name: fmt.Sprintf("small-job-%d", j), RequiredGPUsPerTask: 1, Priority: lowPriority,
			QueueName: "queue0", JobAgeInMinutes: 500 - j,
			Tasks: []*tasks_fake.TestTaskBasic{{NodeName: fmt.Sprintf("gb200-r0-n%d", j/4), State: pod_status.Running}},
		})
	}
	rtxTaken := map[string]int{}
	if tc.elasticVictim {
		elastic := subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName, nil)
		elastic.AddPodSet(subgroup_info.NewPodSet(podgroup_info.DefaultSubGroup, 1, nil))
		var tasks []*tasks_fake.TestTaskBasic
		for _, node := range []string{"gb200-r0-n0", "gb200-r0-n1"} {
			for g := 0; g < 4; g++ {
				tasks = append(tasks, &tasks_fake.TestTaskBasic{NodeName: node, State: pod_status.Running})
			}
		}
		for g := 0; g < 2; g++ {
			tasks = append(tasks, &tasks_fake.TestTaskBasic{NodeName: "rtx-n0", State: pod_status.Running})
		}
		rtxTaken["rtx-n0"] = 2
		jobs = append(jobs, &jobs_fake.TestJobBasic{
			Name: "elastic-job", RequiredGPUsPerTask: 1, Priority: lowPriority, QueueName: "queue0",
			JobAgeInMinutes: 50, RootSubGroupSet: elastic, Tasks: tasks,
		})
	}
	for n := 0; n < tc.rtxNodes; n++ {
		name := fmt.Sprintf("rtx-n%d", n)
		nodes[name] = nodes_fake.TestNodeBasic{GPUs: tc.rtxGPUs, CPUMillis: 64, CPUMemory: 1e12,
			Labels: map[string]string{acceleratorLabel: rtx, archLabel: "amd64"}}
		poolOf[name] = rtx
		for g := 0; g < tc.rtxGPUs-rtxTaken[name]; g++ {
			rtxAge--
			prio := highPriority
			if g < tc.rtxLowGPUs {
				prio = lowPriority
			}
			rtxGPUsPerTask := float64(1)
			if tc.rtxCPUOnly {
				rtxGPUsPerTask = 0
			}
			jobs = append(jobs, &jobs_fake.TestJobBasic{
				Name: fmt.Sprintf("rtx-job-%s-%d", name, g), RequiredGPUsPerTask: rtxGPUsPerTask,
				RequiredCPUsPerTask: 4, RequiredMemoryPerTask: 1e9, Priority: prio,
				QueueName: "queue0", JobAgeInMinutes: rtxAge,
				Tasks: []*tasks_fake.TestTaskBasic{{NodeName: name, State: pod_status.Running}},
			})
		}
	}

	root := subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName, nil)
	var constraint *topology_info.TopologyConstraintInfo
	var topologies []*kaiv1alpha1.Topology
	if tc.withTopology {
		constraint = &topology_info.TopologyConstraintInfo{Topology: "a4x-default", RequiredLevel: subblockLabel}
		topologies = []*kaiv1alpha1.Topology{{
			ObjectMeta: metav1.ObjectMeta{Name: "a4x-default"},
			Spec:       kaiv1alpha1.TopologySpec{Levels: []kaiv1alpha1.TopologyLevel{{NodeLabel: subblockLabel}}},
		}}
	}
	var gbNodeNames []string
	for name := range nodes {
		if poolOf[name] == gb200 {
			gbNodeNames = append(gbNodeNames, name)
		}
	}
	sort.Strings(gbNodeNames)
	var gang []*tasks_fake.TestTaskBasic
	if tc.gangNodes > 0 {
		root.AddPodSet(subgroup_info.NewPodSet("rack", int32(tc.gangNodes), constraint))
	}
	for i := 0; i < tc.gangNodes; i++ {
		task := &tasks_fake.TestTaskBasic{State: pod_status.Pending, SubGroupName: "rack"}
		if tc.gangGPUs > 0 {
			task.RequiredGPUs = ptr.To(int64(tc.gangGPUs))
		}
		switch tc.constraint {
		case byNodeSelector:
			task.NodeSelector = map[string]string{acceleratorLabel: gb200}
		case byNodeAffinity:
			task.NodeAffinityNames = gbNodeNames
		case byArch:
			task.NodeSelector = map[string]string{archLabel: "arm64"}
		case byTaint:
		}
		gang = append(gang, task)
	}
	if tc.rtxPodSet > 0 {
		root.AddPodSet(subgroup_info.NewPodSet("rtx", int32(tc.rtxPodSet), nil))
	}
	for i := 0; i < tc.rtxPodSet; i++ {
		gang = append(gang, &tasks_fake.TestTaskBasic{
			State: pod_status.Pending, SubGroupName: "rtx", RequiredGPUs: ptr.To(int64(1)),
			NodeSelector: map[string]string{acceleratorLabel: rtx},
		})
	}
	if tc.cpuPodSet {
		root.AddPodSet(subgroup_info.NewPodSet("cpu", 1, nil))
		gang = append(gang, &tasks_fake.TestTaskBasic{
			State: pod_status.Pending, SubGroupName: "cpu", RequiredGPUs: ptr.To(int64(0)),
			NodeSelector: map[string]string{archLabel: "arm64"},
		})
	}
	if tc.rtxPreemptor > 0 {
		root.AddPodSet(subgroup_info.NewPodSet("rtx", 1, nil))
		gang = append(gang, &tasks_fake.TestTaskBasic{
			State: pod_status.Pending, SubGroupName: "rtx", RequiredGPUs: ptr.To(int64(tc.rtxPreemptor)),
			NodeSelector: map[string]string{acceleratorLabel: rtx},
		})
	}
	jobs = append(jobs, &jobs_fake.TestJobBasic{
		Name: "preemptor", RequiredGPUsPerTask: 4, Priority: mediumPriority, QueueName: "queue0",
		JobAgeInMinutes: 1, RootSubGroupSet: root, Tasks: gang,
	})

	topo := test_utils.TestTopologyBasic{
		Name: tc.name, Nodes: nodes, Jobs: jobs, Topologies: topologies,
		Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 1e6, GPUOverQuotaWeight: 1,
			MaxAllowedGPUs: -1}},
		Mocks: &test_utils.TestMock{
			CacheRequirements: &test_utils.CacheMocking{NumberOfPipelineActions: 1000},
			SchedulerConf:     voyagerSchedulerConf(),
		},
	}
	ssn := test_utils.BuildSession(topo, controller)
	cordoned := map[string]bool{}
	for _, r := range tc.cordonedRacks {
		cordoned[fmt.Sprintf("rack%d", r)] = true
	}
	for name, node := range ssn.ClusterInfo.Nodes {
		if tc.constraint == byTaint && poolOf[name] == rtx {
			node.Node.Spec.Taints = append(node.Node.Spec.Taints,
				v1.Taint{Key: "pool", Value: rtx, Effect: v1.TaintEffectNoSchedule})
		}
		if cordoned[node.Node.Labels[subblockLabel]] {
			node.Node.Spec.Unschedulable = true
		}
	}
	preemptorJob := ssn.ClusterInfo.PodGroupInfos["preemptor"]
	if tc.tolerateCordon {
		for _, task := range preemptorJob.GetAllPodsMap() {
			task.Pod.Spec.Tolerations = append(task.Pod.Spec.Tolerations, v1.Toleration{
				Key: v1.TaintNodeUnschedulable, Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule})
		}
	}

	// Record the pods that preempt really deletes: the eviction calls made on commit.
	evicted := map[string]string{}
	ssn.Cache.(*cache.MockCache).EXPECT().Evict(Any(), Any(), Any(), Any()).
		DoAndReturn(func(pod *v1.Pod, _ *podgroup_info.PodGroupInfo, _ eviction_info.EvictionMetadata, _ string) error {
			evicted[pod.GetName()] = ""
			return nil
		}).AnyTimes()

	nodeOf := map[string]string{}
	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		for _, task := range job.GetAllPodsMap() {
			nodeOf[task.Name] = task.NodeName
		}
	}
	preempt.New().Execute(ssn)

	byPool := map[string]int{}
	for pod := range evicted {
		byPool[poolOf[nodeOf[pod]]]++
	}
	placed := 0
	for _, task := range preemptorJob.GetAllPodsMap() {
		if task.Status == pod_status.Pipelined {
			placed++
		}
	}
	t.Logf("%s: %d evictions %v, gang placed %d/%d", tc.name, len(evicted), byPool, placed, len(gang))
	return byPool, placed, len(gang)
}

// voyagerSchedulerConf is the voyager-7 scheduler configuration (plugins and order as in the
// live snapshot), which differs from the default test configuration.
func voyagerSchedulerConf() *conf.SchedulerConfiguration {
	var plugins []conf.PluginOption
	for _, name := range []string{
		"predicates", "proportion", "priority", "nodeavailability", "resourcetype", "podaffinity", "elastic",
		"kubeflow", "ray", "subgrouporder", "taskorder", "nominatednode", "dynamicresources", "minruntime",
		"topology", "snapshot", "sg-nodelocalgreedy", "sg-multinodegang", "gpupack", "nodeplacement",
		"gpusharingorder", "backgroundpods", "numa",
	} {
		option := conf.PluginOption{Name: name}
		if name == "nodeplacement" {
			option.Arguments = map[string]string{"cpu": "binpack", "gpu": "binpack"}
		}
		plugins = append(plugins, option)
	}
	return &conf.SchedulerConfiguration{
		Actions: "allocate, reclaim, preempt, stalegangeviction",
		Tiers:   []conf.Tier{{Plugins: plugins}},
	}
}
