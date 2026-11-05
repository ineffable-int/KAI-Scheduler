// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

// Package reeviction_test reproduces the "re-eviction loop": while the victims of an earlier preemption are
// still terminating (Releasing), later scheduling sessions evict a fresh batch of victims for the same
// pending job.
//
// The test runs several consecutive sessions (allocate, reclaim, preempt, stalegangeviction - the production
// action list, consolidation is off) over a fake cluster. Between sessions, victims that a session evicted are
// kept as Releasing pods that still hold their node resources and, in the DRA variants, their ResourceClaim
// allocation (a terminating pod's claim stays allocated until the pod is gone). The preemptor, which a session
// can only pipeline (never bind) while its victims terminate, goes back to Pending. Each case lists the number
// of evictions it expects per session.
//
// The expected values describe the DESIRED behaviour (a pending job never causes more evictions while the
// resources it already freed are still being released), so cases that hit the loop fail on a scheduler
// without a fix. To change what a case expects, edit its wantEvictions / wantGangRunning fields.
package reeviction_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "go.uber.org/mock/gomock"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	featuregates "github.com/kai-scheduler/KAI-scheduler/pkg/common/feature_gates"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/stalegangeviction"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/dra_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

const (
	gpusPerNode   = 4
	queueName     = "default-queue"
	namespace     = "fly"
	channelClass  = "compute-domain-default-channel.nvidia.com"
	channelDriver = "compute-domain.nvidia.com"

	// fly-low and fly-medium priority classes.
	victimPriority    = 25
	preemptorPriority = 50

	gangName = "gang"

	hostnameLabel = "kubernetes.io/hostname"
)

// scenario is one table row. Cluster: `nodes` nodes of 4 GPUs, each fully used by one single-pod low-priority
// victim job. Preemptor: a medium-priority gang of `gangSize` pods of 4 GPUs each, pending.
type scenario struct {
	name     string
	nodes    int
	gangSize int
	// withChannelClaim: every pod has a ResourceClaim (from a template) for the node's single DRA device
	// "channel-0", as GB200 training pods do.
	withChannelClaim bool
	// withAntiAffinity: every gang pod has required pod anti-affinity (hostname topology) against the victims'
	// pods, so it can only be placed on a node once the victim pod is gone.
	withAntiAffinity bool
	cycles           int
	// terminatingForCycles: how many sessions the evicted victims stay Releasing before the pods are gone
	// (and their claims freed). 0 means they terminate forever (the worst case, isolates the scheduler logic).
	terminatingForCycles int

	// Knobs under evaluation.
	queuePreemptMinRuntime time.Duration
	victimRunningFor       func(victimIndex int) time.Duration // default: 2h, i.e. not protected by any min runtime
	preemptionDelay        time.Duration
	preemptorAge           time.Duration // default 30m
	pluginArguments        map[string]map[string]string

	// Expected outcome: evictions in each session, and whether the gang runs at the end.
	wantEvictions   []int
	wantGangRunning bool
}

// noLoop is the desired behaviour: the gang's first batch of victims is evicted in session 1 and nothing
// more is evicted afterwards.
func noLoop(firstBatch, cycles int) []int {
	evictions := make([]int, cycles)
	evictions[0] = firstBatch
	return evictions
}

func scenarios() []scenario {
	const nodes, gang, cycles = 16, 4, 5
	pipelineOverReleasing := map[string]map[string]string{
		"dynamicresources": {"pipelineOverReleasingClaims": "true"},
	}
	withClaim := func(s scenario) scenario {
		s.nodes, s.gangSize, s.cycles, s.withChannelClaim = nodes, gang, cycles, true
		return s
	}
	plain := func(s scenario) scenario {
		s.nodes, s.gangSize, s.cycles = nodes, gang, cycles
		return s
	}
	withAffinity := func(s scenario) scenario {
		s.nodes, s.gangSize, s.cycles, s.withAntiAffinity = nodes, gang, cycles, true
		return s
	}
	return []scenario{
		plain(scenario{
			name:          "gpus only (control)",
			wantEvictions: noLoop(gang, cycles),
		}),
		withAffinity(scenario{
			name:          "gpus + required anti-affinity against the victims",
			wantEvictions: noLoop(gang, cycles),
		}),
		withAffinity(scenario{
			name:                 "gpus + required anti-affinity, victims gone after 2 sessions",
			terminatingForCycles: 2,
			wantEvictions:        noLoop(gang, cycles),
			wantGangRunning:      true,
		}),
		withClaim(scenario{
			name:          "gpus + channel claim",
			wantEvictions: noLoop(gang, cycles),
		}),
		withClaim(scenario{
			name:                 "gpus + channel claim, victims gone after 2 sessions",
			terminatingForCycles: 2,
			wantEvictions:        noLoop(gang, cycles),
			wantGangRunning:      true,
		}),
		withClaim(scenario{
			name:                   "gpus + channel claim, queue preemptMinRuntime 10m, victims running 2h",
			queuePreemptMinRuntime: 10 * time.Minute,
			wantEvictions:          noLoop(gang, cycles),
		}),
		withClaim(scenario{
			name:                   "gpus + channel claim, queue preemptMinRuntime 10m, victims running 1m (all protected)",
			queuePreemptMinRuntime: 10 * time.Minute,
			victimRunningFor:       func(int) time.Duration { return time.Minute },
			wantEvictions:          make([]int, cycles), // nothing evictable: the gang starves, but no loop
		}),
		withClaim(scenario{
			name:                   "gpus + channel claim, queue preemptMinRuntime 10m, 10 of 16 victims old",
			queuePreemptMinRuntime: 10 * time.Minute,
			victimRunningFor: func(i int) time.Duration {
				if i < 10 {
					return 2 * time.Hour
				}
				return time.Minute
			},
			wantEvictions: noLoop(gang, cycles), // loop bounded by the 10 unprotected victims, still a loop
		}),
		withClaim(scenario{
			name:            "gpus + channel claim, preemptionDelay 10m, gang created 1m ago",
			preemptionDelay: 10 * time.Minute,
			preemptorAge:    time.Minute,
			wantEvictions:   make([]int, cycles), // gang may not trigger evictions yet
		}),
		withClaim(scenario{
			name:            "gpus + channel claim, preemptionDelay 1m, gang created 30m ago (delay elapsed)",
			preemptionDelay: time.Minute,
			wantEvictions:   noLoop(gang, cycles),
		}),
		withClaim(scenario{
			name:            "gpus + channel claim, pipelineOverReleasingClaims=true",
			pluginArguments: pipelineOverReleasing,
			wantEvictions:   noLoop(gang, cycles),
		}),
		withClaim(scenario{
			name:                 "gpus + channel claim, pipelineOverReleasingClaims=true, victims gone after 2 sessions",
			pluginArguments:      pipelineOverReleasing,
			terminatingForCycles: 2,
			wantEvictions:        noLoop(gang, cycles),
			wantGangRunning:      true,
		}),
	}
}

func TestReevictionLoop(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	featuregates.SetDynamicResourcesEnabledForTest(true)
	t.Cleanup(func() { featuregates.SetDynamicResourcesEnabledForTest(false) })

	for _, s := range scenarios() {
		t.Run(s.name, func(t *testing.T) {
			controller := NewController(t)
			defer controller.Finish()

			result := runSessions(t, controller, s)
			t.Logf("evictions per session: %v, gang after each session: %v", result.evictions, result.gangStates)

			if !equalInts(result.evictions, s.wantEvictions) {
				t.Errorf("evictions per session = %v, want %v", result.evictions, s.wantEvictions)
			}
			gangRunning := result.gangStates[len(result.gangStates)-1] == "Running"
			if gangRunning != s.wantGangRunning {
				t.Errorf("gang running at the end = %v, want %v (states: %v)",
					gangRunning, s.wantGangRunning, result.gangStates)
			}
		})
	}
}

type sessionsResult struct {
	evictions  []int
	gangStates []string
}

// cluster is the mutable fake cluster carried from one session to the next.
type cluster struct {
	topology test_utils.TestTopologyBasic
	// terminatingSince: victim job name -> 1-based session in which the victim was evicted.
	terminatingSince map[string]int
}

func runSessions(t *testing.T, controller *Controller, s scenario) sessionsResult {
	c := buildCluster(s)
	var result sessionsResult
	for cycle := 1; cycle <= s.cycles; cycle++ {
		c.deleteTerminatedVictims(s, cycle)

		ssn := test_utils.BuildSession(c.topology, controller)
		applyJobOverrides(ssn, s)
		for _, action := range []framework.Action{
			allocate.New(), reclaim.New(), preempt.New(), stalegangeviction.New(),
		} {
			action.Execute(ssn)
		}

		evicted := c.advance(ssn, cycle)
		result.evictions = append(result.evictions, evicted)
		result.gangStates = append(result.gangStates, gangState(ssn))
	}
	return result
}

func buildCluster(s scenario) *cluster {
	topology := test_utils.TestTopologyBasic{
		Name:   s.name,
		Nodes:  map[string]nodes_fake.TestNodeBasic{},
		Queues: []test_utils.TestQueueBasic{{Name: queueName, DeservedGPUs: float64(s.nodes * gpusPerNode)}},
		Mocks: &test_utils.TestMock{
			CacheRequirements: &test_utils.CacheMocking{
				NumberOfCacheBinds: 1000, NumberOfCacheEvictions: 1000, NumberOfPipelineActions: 1000,
			},
			SchedulerConf: schedulerConfig(s),
		},
	}
	if s.queuePreemptMinRuntime > 0 {
		topology.Queues[0].PreemptMinRuntime = &metav1.Duration{Duration: s.queuePreemptMinRuntime}
	}
	if s.withChannelClaim {
		topology.DeviceClasses = []string{channelClass}
	}

	for i := 0; i < s.nodes; i++ {
		nodeName := nodeNameOf(i)
		topology.Nodes[nodeName] = nodes_fake.TestNodeBasic{
			GPUs: gpusPerNode, Labels: map[string]string{hostnameLabel: nodeName},
		}

		victim := &jobs_fake.TestJobBasic{
			Name:                victimName(i),
			Namespace:           namespace,
			QueueName:           queueName,
			Priority:            victimPriority,
			RequiredGPUsPerTask: gpusPerNode,
			JobAgeInMinutes:     180,
			Tasks:               []*tasks_fake.TestTaskBasic{{NodeName: nodeName, State: pod_status.Running}},
		}
		if s.withAntiAffinity {
			victim.Tasks[0].PodAffinityLabels = map[string]string{"workload": "victim"}
		}
		if s.withChannelClaim {
			claim := podClaimName(victim.Name, 0)
			victim.Tasks[0].ResourceClaimTemplates = map[string]string{"channel": claim}
			topology.ResourceClaims = append(topology.ResourceClaims, channelClaim(claim, nodeName, victim.Name+"-0"))
			topology.ResourceSlices = append(topology.ResourceSlices, &dra_fake.TestResourceSlice{
				Name: nodeName + "-channel", DeviceClassName: channelClass, NodeName: nodeName, Count: 1, Driver: channelDriver,
			})
		}
		topology.Jobs = append(topology.Jobs, victim)
	}

	preemptorAge := s.preemptorAge
	if preemptorAge == 0 {
		preemptorAge = 30 * time.Minute
	}
	gang := &jobs_fake.TestJobBasic{
		Name:                gangName,
		Namespace:           namespace,
		QueueName:           queueName,
		Priority:            preemptorPriority,
		RequiredGPUsPerTask: gpusPerNode,
		JobAgeInMinutes:     int(preemptorAge / time.Minute),
	}
	if s.preemptionDelay > 0 {
		gang.PreemptionDelay = &metav1.Duration{Duration: s.preemptionDelay}
	}
	for i := 0; i < s.gangSize; i++ {
		task := &tasks_fake.TestTaskBasic{State: pod_status.Pending}
		if s.withAntiAffinity {
			task.PodAntiAffinitySelector = map[string]string{"workload": "victim"}
			task.PodAntiAffinityTopologyKey = hostnameLabel
		}
		if s.withChannelClaim {
			claim := podClaimName(gangName, i)
			task.ResourceClaimTemplates = map[string]string{"channel": claim}
			topology.ResourceClaims = append(topology.ResourceClaims, &dra_fake.TestResourceClaim{
				Name: claim, Namespace: namespace, DeviceClassName: channelClass, Count: 1,
			})
		}
		gang.Tasks = append(gang.Tasks, task)
	}
	topology.Jobs = append(topology.Jobs, gang)

	return &cluster{topology: topology, terminatingSince: map[string]int{}}
}

// schedulerConfig is the production action list (consolidation off) and plugin list, with optional
// plugin arguments.
func schedulerConfig(s scenario) *conf.SchedulerConfiguration {
	config := &conf.SchedulerConfiguration{Actions: "allocate, reclaim, preempt, stalegangeviction"}
	var plugins []conf.PluginOption
	for _, name := range []string{
		"predicates", "proportion", "priority", "nodeavailability", "resourcetype", "podaffinity", "elastic",
		"subgrouporder", "taskorder", "nominatednode", "dynamicresources", "minruntime", "topology",
		"sg-nodelocalgreedy", "sg-multinodegang", "gpupack", "nodeplacement",
	} {
		option := conf.PluginOption{Name: name, Arguments: s.pluginArguments[name]}
		if name == "nodeplacement" {
			option.Arguments = map[string]string{"cpu": "binpack", "gpu": "binpack"}
		}
		plugins = append(plugins, option)
	}
	config.Tiers = []conf.Tier{{Plugins: plugins}}
	return config
}

// applyJobOverrides sets what the fake job builders do not expose: how long each victim has been running.
func applyJobOverrides(ssn *framework.Session, s scenario) {
	for i := 0; i < s.nodes; i++ {
		job := ssn.ClusterInfo.PodGroupInfos[jobID(victimName(i))]
		if job == nil {
			continue // already gone
		}
		runningFor := 2 * time.Hour
		if s.victimRunningFor != nil {
			runningFor = s.victimRunningFor(i)
		}
		started := time.Now().Add(-runningFor)
		job.LastStartTimestamp = &started
	}
}

// advance records what the finished session did and turns it into the next session's cluster state.
// It returns the number of pods the session evicted.
func (c *cluster) advance(ssn *framework.Session, cycle int) int {
	evicted := 0
	claimStatuses := sessionClaimStatuses(ssn)
	for _, job := range c.topology.Jobs {
		sessionJob := ssn.ClusterInfo.PodGroupInfos[jobID(job.Name)]
		for taskIndex, task := range job.Tasks {
			sessionTask := sessionJob.GetAllPodsMap()[podID(job.Name, taskIndex)]
			if task.State == pod_status.Running && !isRunning(sessionTask.Status) {
				// The session evicted this pod (it may have re-placed the replacement as Pipelined).
				// The pod keeps its node, GPUs and claim while it terminates.
				evicted++
				c.terminatingSince[job.Name] = cycle
				task.State = pod_status.Releasing
				continue
			}
			switch sessionTask.Status {
			case pod_status.Releasing:
			case pod_status.Pipelined, pod_status.Pending:
				task.State, task.NodeName = pod_status.Pending, ""
				c.setClaimStatus(job.Name, taskIndex, nil)
			case pod_status.Allocated, pod_status.Binding, pod_status.Bound, pod_status.Running:
				task.State, task.NodeName = pod_status.Running, sessionTask.NodeName
				c.setClaimStatus(job.Name, taskIndex, claimStatuses[podClaimName(job.Name, taskIndex)])
			default:
				panic(fmt.Sprintf("unexpected status %v of %s", sessionTask.Status, sessionTask.Name))
			}
		}
	}
	return evicted
}

// deleteTerminatedVictims removes victims that were evicted terminatingForCycles sessions ago: the pod is
// gone, so the node's GPUs and the pod's claim are freed.
func (c *cluster) deleteTerminatedVictims(s scenario, cycle int) {
	if s.terminatingForCycles == 0 {
		return
	}
	var jobs []*jobs_fake.TestJobBasic
	for _, job := range c.topology.Jobs {
		since, terminating := c.terminatingSince[job.Name]
		if terminating && cycle-since >= s.terminatingForCycles {
			c.dropClaims(job.Name)
			delete(c.terminatingSince, job.Name)
			continue
		}
		jobs = append(jobs, job)
	}
	c.topology.Jobs = jobs
}

func (c *cluster) dropClaims(jobName string) {
	var claims []*dra_fake.TestResourceClaim
	for _, claim := range c.topology.ResourceClaims {
		if !strings.HasPrefix(claim.Name, jobName+"-") {
			claims = append(claims, claim)
		}
	}
	c.topology.ResourceClaims = claims
}

func (c *cluster) setClaimStatus(jobName string, taskIndex int, status *resourceapi.ResourceClaimStatus) {
	for _, claim := range c.topology.ResourceClaims {
		if claim.Name == podClaimName(jobName, taskIndex) {
			claim.ClaimStatus = status
		}
	}
}

// sessionClaimStatuses returns the allocation the session's DRA manager holds for every claim.
func sessionClaimStatuses(ssn *framework.Session) map[string]*resourceapi.ResourceClaimStatus {
	statuses := map[string]*resourceapi.ResourceClaimStatus{}
	k8sPlugins := ssn.InternalK8sPlugins()
	if k8sPlugins == nil || k8sPlugins.SessionDRAManager == nil {
		return statuses
	}
	claims, err := k8sPlugins.SessionDRAManager.ResourceClaims().List()
	if err != nil {
		panic(err)
	}
	for _, claim := range claims {
		if claim.Status.Allocation != nil {
			statuses[claim.Name] = claim.Status.DeepCopy()
		}
	}
	return statuses
}

func isRunning(status pod_status.PodStatus) bool {
	switch status {
	case pod_status.Allocated, pod_status.Binding, pod_status.Bound, pod_status.Running:
		return true
	}
	return false
}

func gangState(ssn *framework.Session) string {
	counts := map[pod_status.PodStatus]int{}
	for _, task := range ssn.ClusterInfo.PodGroupInfos[jobID(gangName)].GetAllPodsMap() {
		switch task.Status {
		case pod_status.Allocated, pod_status.Binding, pod_status.Bound, pod_status.Running:
			counts[pod_status.Running]++
		default:
			counts[task.Status]++
		}
	}
	for _, status := range []pod_status.PodStatus{pod_status.Running, pod_status.Pipelined, pod_status.Pending} {
		if counts[status] > 0 {
			return status.String()
		}
	}
	return "Unknown"
}

func channelClaim(name, nodeName, podName string) *dra_fake.TestResourceClaim {
	return &dra_fake.TestResourceClaim{
		Name: name, Namespace: namespace, DeviceClassName: channelClass, Count: 1,
		ClaimStatus: &resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Request: "request", Driver: channelDriver, Pool: nodeName, Device: "0"},
					},
				},
			},
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{Resource: "pods", Name: podName, UID: podUID(podName)},
			},
		},
	}
}

func podUID(podName string) types.UID { return types.UID(podName) }

func jobID(name string) common_info.PodGroupID { return common_info.NewPodGroupID(namespace, name) }

func podID(job string, taskIndex int) common_info.PodID {
	return common_info.PodID(fmt.Sprintf("%s-%d", job, taskIndex))
}

func nodeNameOf(i int) string { return fmt.Sprintf("node-%02d", i) }
func victimName(i int) string { return fmt.Sprintf("victim-%02d", i) }
func podClaimName(job string, taskIndex int) string {
	return fmt.Sprintf("%s-%d-channel", job, taskIndex)
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
