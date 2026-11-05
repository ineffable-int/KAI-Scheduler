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

	"github.com/kai-scheduler/KAI-scheduler/pkg/common/constants"
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

	gangName       = "gang"
	secondGangName = "gang-b"
	staleVictim    = "stale-victim"

	defaultSessionPeriod = 10 * time.Second

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
	// preemptCooldown is the scheduler's --preempt-cooldown: nil means its default, 0 disables the guard.
	preemptCooldown *time.Duration
	// sessionPeriod is the simulated time between two sessions (default 10s). It ages the jobs' preemption
	// timestamps, the victims' deletion timestamps and how long the victims have been running.
	sessionPeriod time.Duration
	// gangMinAvailable makes the gang elastic: only this many of its pods are needed to start (default: all).
	gangMinAvailable int
	// secondGangFromCycle > 0 creates a second, identical pending gang before that session.
	secondGangFromCycle int
	// withStaleVictim adds a node running one pod of a two-pod victim gang whose other pod is pending, i.e. a
	// stale gang that stalegangeviction evicts.
	withStaleVictim bool

	// Expected outcome: evictions in each session, and whether the gang (and the second gang) runs at the end.
	wantEvictions         []int
	wantGangRunning       bool
	wantSecondGangRunning bool
	// wantGangStates, when set, is the gang's state after each session.
	wantGangStates []string
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
	guardOff := time.Duration(0)
	pipelineOverReleasing := map[string]map[string]string{
		"dynamicresources": {"pipelineOverReleasingClaims": "true"},
	}
	plain := func(s scenario) scenario {
		s.nodes = nodes
		if s.gangSize == 0 {
			s.gangSize = gang
		}
		if s.cycles == 0 {
			s.cycles = cycles
		}
		return s
	}
	withClaim := func(s scenario) scenario {
		s.withChannelClaim = true
		return plain(s)
	}
	withAffinity := func(s scenario) scenario {
		s.withAntiAffinity = true
		return plain(s)
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

		// Guard off (--preempt-cooldown=0): the deployed scheduler's behaviour.
		withClaim(scenario{
			name:            "guard off: gpus + channel claim",
			preemptCooldown: &guardOff,
			wantEvictions:   []int{4, 4, 4, 4, 0},
		}),
		withAffinity(scenario{
			name:            "guard off: gpus + required anti-affinity",
			preemptCooldown: &guardOff,
			wantEvictions:   []int{4, 4, 4, 4, 0},
		}),
		withClaim(scenario{
			name:                 "guard off: gpus + channel claim, victims gone after 2 sessions",
			preemptCooldown:      &guardOff,
			terminatingForCycles: 2,
			wantEvictions:        []int{4, 4, 0, 0, 0},
			wantGangRunning:      true,
		}),

		// Victims finish before the window ends: the gang is placed as soon as they are gone, and allocate keeps
		// pipelining it onto the releasing GPUs meanwhile.
		withClaim(scenario{
			name:                 "gpus + channel claim, victims gone after 1 session",
			terminatingForCycles: 1,
			wantEvictions:        noLoop(gang, cycles),
			wantGangRunning:      true,
			wantGangStates:       []string{"Pipelined", "Running", "Running", "Running", "Running"},
		}),
		plain(scenario{
			name:                 "gpus only, victims gone after 2 sessions (pipelined during the window)",
			terminatingForCycles: 2,
			wantEvictions:        noLoop(gang, cycles),
			wantGangRunning:      true,
			wantGangStates:       []string{"Pipelined", "Pipelined", "Running", "Running", "Running"},
		}),

		// The window bounds the wait: with 1-minute sessions and the 3-minute default, the gang may preempt again
		// in session 4 if its victims are still terminating, not before.
		withClaim(scenario{
			name:          "gpus + channel claim, victims never finish, 1m sessions",
			sessionPeriod: time.Minute,
			cycles:        10,
			wantEvictions: []int{4, 0, 0, 4, 0, 0, 4, 0, 0, 4},
		}),
		withClaim(scenario{
			name:                 "gpus + channel claim, victims stuck terminating 5m, 1m sessions",
			sessionPeriod:        time.Minute,
			cycles:               8,
			terminatingForCycles: 5,
			wantEvictions:        []int{4, 0, 0, 4, 0, 0, 0, 0},
			wantGangRunning:      true,
		}),

		// An elastic gang that needs more victims after its first round.
		plain(scenario{
			name:                 "elastic gang (4 of 8 pods), victims gone after 2 sessions",
			gangSize:             8,
			gangMinAvailable:     4,
			terminatingForCycles: 2,
			wantEvictions:        []int{4, 0, 1, 0, 1}, // elastic pods are preempted for one at a time
			wantGangRunning:      true,
		}),
		plain(scenario{
			name:             "elastic gang (4 of 8 pods), victims never finish, 1m sessions",
			gangSize:         8,
			gangMinAvailable: 4,
			sessionPeriod:    time.Minute,
			cycles:           7,
			wantEvictions:    []int{4, 0, 0, 1, 0, 0, 1},
		}),

		// The window is per job: a second gang still preempts while the first one waits.
		withClaim(scenario{
			name:                  "second gang created in session 2",
			secondGangFromCycle:   2,
			terminatingForCycles:  3,
			wantEvictions:         []int{4, 4, 0, 0, 0},
			wantGangRunning:       true,
			wantSecondGangRunning: true,
		}),

		// preemptionDelay and minruntime still apply and don't combine with the window into starvation.
		withClaim(scenario{
			name:                 "preemptionDelay 2m, gang created 1m ago, 1m sessions, victims gone after 2",
			preemptionDelay:      2 * time.Minute,
			preemptorAge:         time.Minute,
			sessionPeriod:        time.Minute,
			terminatingForCycles: 2,
			wantEvictions:        []int{0, 4, 0, 0, 0},
			wantGangRunning:      true,
		}),
		withClaim(scenario{
			name:                   "queue preemptMinRuntime 10m, victims running 9m, 1m sessions, victims gone after 2",
			queuePreemptMinRuntime: 10 * time.Minute,
			victimRunningFor:       func(int) time.Duration { return 9 * time.Minute },
			sessionPeriod:          time.Minute,
			terminatingForCycles:   2,
			wantEvictions:          []int{0, 4, 0, 0, 0},
			wantGangRunning:        true,
		}),

		// stalegangeviction is not an eviction-triggering job action and is not affected by the window.
		withClaim(scenario{
			name:            "stale victim gang",
			withStaleVictim: true,
			wantEvictions:   []int{5, 0, 0, 0, 0},
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
			t.Logf("evictions per session: %v, gang after each session: %v, second gang: %v",
				result.evictions, result.gangStates, result.secondGangStates)

			if !equalInts(result.evictions, s.wantEvictions) {
				t.Errorf("evictions per session = %v, want %v", result.evictions, s.wantEvictions)
			}
			gangRunning := result.gangStates[len(result.gangStates)-1] == "Running"
			if gangRunning != s.wantGangRunning {
				t.Errorf("gang running at the end = %v, want %v (states: %v)",
					gangRunning, s.wantGangRunning, result.gangStates)
			}
			if s.wantGangStates != nil && strings.Join(result.gangStates, ",") != strings.Join(s.wantGangStates, ",") {
				t.Errorf("gang states = %v, want %v", result.gangStates, s.wantGangStates)
			}
			if s.secondGangFromCycle > 0 {
				running := result.secondGangStates[len(result.secondGangStates)-1] == "Running"
				if running != s.wantSecondGangRunning {
					t.Errorf("second gang running at the end = %v, want %v (states: %v)",
						running, s.wantSecondGangRunning, result.secondGangStates)
				}
			}
		})
	}
}

type sessionsResult struct {
	evictions        []int
	gangStates       []string
	secondGangStates []string
}

// cluster is the mutable fake cluster carried from one session to the next.
type cluster struct {
	topology test_utils.TestTopologyBasic
	// terminatingSince: victim job name -> 1-based session in which the victim was evicted.
	terminatingSince map[string]int
	// preemptedAt: job name -> 1-based session in which a preemption was committed for the job, read back from
	// cooldowns, the scheduler's in-memory store and the only state it keeps between sessions. The store is
	// rewritten before each session to the session's simulated clock.
	preemptedAt map[string]int
	cooldowns   *conf.PreemptCooldowns
}

func runSessions(t *testing.T, controller *Controller, s scenario) sessionsResult {
	c := buildCluster(s)
	var result sessionsResult
	for cycle := 1; cycle <= s.cycles; cycle++ {
		if cycle == s.secondGangFromCycle {
			c.addGang(s, secondGangName)
		}
		c.deleteTerminatedVictims(s, cycle)

		ssn := test_utils.BuildSession(c.topology, controller)
		ssn.SchedulerParams.PreemptCooldown = constants.DefaultPreemptCooldown
		ssn.SchedulerParams.PreemptCooldowns = c.cooldowns
		if s.preemptCooldown != nil {
			ssn.SchedulerParams.PreemptCooldown = *s.preemptCooldown
		}
		sessionStart := time.Now()
		c.applyJobOverrides(ssn, s, cycle, sessionStart)
		for _, action := range []framework.Action{
			allocate.New(), reclaim.New(), preempt.New(), stalegangeviction.New(),
		} {
			action.Execute(ssn)
		}

		evicted := c.advance(ssn, cycle, sessionStart)
		result.evictions = append(result.evictions, evicted)
		result.gangStates = append(result.gangStates, gangState(ssn, gangName))
		if s.secondGangFromCycle > 0 {
			result.secondGangStates = append(result.secondGangStates, gangState(ssn, secondGangName))
		}
	}
	return result
}

func sessionPeriod(s scenario) time.Duration {
	if s.sessionPeriod > 0 {
		return s.sessionPeriod
	}
	return defaultSessionPeriod
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

	if s.withStaleVictim {
		nodeName := nodeNameOf(s.nodes)
		topology.Nodes[nodeName] = nodes_fake.TestNodeBasic{
			GPUs: gpusPerNode, Labels: map[string]string{hostnameLabel: nodeName},
		}
		if s.withChannelClaim {
			topology.ResourceSlices = append(topology.ResourceSlices, &dra_fake.TestResourceSlice{
				Name: nodeName + "-channel", DeviceClassName: channelClass, NodeName: nodeName, Count: 1, Driver: channelDriver,
			})
		}
		staleFor := 10 * time.Minute
		topology.Jobs = append(topology.Jobs, &jobs_fake.TestJobBasic{
			Name:                staleVictim,
			Namespace:           namespace,
			QueueName:           queueName,
			Priority:            victimPriority,
			RequiredGPUsPerTask: gpusPerNode,
			JobAgeInMinutes:     180,
			RootSubGroupSet:     jobs_fake.DefaultSubGroup(2),
			StaleDuration:       &staleFor,
			Tasks: []*tasks_fake.TestTaskBasic{
				{NodeName: nodeName, State: pod_status.Running}, {State: pod_status.Pending},
			},
		})
	}

	c := &cluster{topology: topology, terminatingSince: map[string]int{}, preemptedAt: map[string]int{},
		cooldowns: conf.NewPreemptCooldowns()}
	c.addGang(s, gangName)
	return c
}

// addGang adds a pending medium-priority gang of s.gangSize pods of 4 GPUs.
func (c *cluster) addGang(s scenario, name string) {
	preemptorAge := s.preemptorAge
	if preemptorAge == 0 {
		preemptorAge = 30 * time.Minute
	}
	gang := &jobs_fake.TestJobBasic{
		Name:                name,
		Namespace:           namespace,
		QueueName:           queueName,
		Priority:            preemptorPriority,
		RequiredGPUsPerTask: gpusPerNode,
		JobAgeInMinutes:     int(preemptorAge / time.Minute),
	}
	if s.preemptionDelay > 0 {
		gang.PreemptionDelay = &metav1.Duration{Duration: s.preemptionDelay}
	}
	if s.gangMinAvailable > 0 {
		gang.RootSubGroupSet = jobs_fake.DefaultSubGroup(int32(s.gangMinAvailable))
	}
	for i := 0; i < s.gangSize; i++ {
		task := &tasks_fake.TestTaskBasic{State: pod_status.Pending}
		if s.withAntiAffinity {
			task.PodAntiAffinitySelector = map[string]string{"workload": "victim"}
			task.PodAntiAffinityTopologyKey = hostnameLabel
		}
		if s.withChannelClaim {
			claim := podClaimName(name, i)
			task.ResourceClaimTemplates = map[string]string{"channel": claim}
			c.topology.ResourceClaims = append(c.topology.ResourceClaims, &dra_fake.TestResourceClaim{
				Name: claim, Namespace: namespace, DeviceClassName: channelClass, Count: 1,
			})
		}
		gang.Tasks = append(gang.Tasks, task)
	}
	c.topology.Jobs = append(c.topology.Jobs, gang)
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

// applyJobOverrides sets what the fake job builders do not expose, as of the session's simulated time: how long
// each victim has been running, how old the gangs are, each job's LastPreemptionTimestamp and each terminating pod's deletion timestamp.
func (c *cluster) applyJobOverrides(ssn *framework.Session, s scenario, cycle int, sessionStart time.Time) {
	period := sessionPeriod(s)
	atSession := func(session int) time.Time {
		return sessionStart.Add(-time.Duration(cycle-session) * period)
	}
	for i := 0; i < s.nodes; i++ {
		job := ssn.ClusterInfo.PodGroupInfos[jobID(victimName(i))]
		if job == nil {
			continue // already gone
		}
		runningFor := 2 * time.Hour
		if s.victimRunningFor != nil {
			runningFor = s.victimRunningFor(i)
		}
		started := atSession(1).Add(-runningFor)
		job.LastStartTimestamp = &started
	}
	preemptorAge := s.preemptorAge
	if preemptorAge == 0 {
		preemptorAge = 30 * time.Minute
	}
	for name, createdIn := range map[string]int{gangName: 1, secondGangName: s.secondGangFromCycle} {
		if job := ssn.ClusterInfo.PodGroupInfos[jobID(name)]; job != nil {
			job.CreationTimestamp = metav1.NewTime(atSession(createdIn).Add(-preemptorAge))
		}
	}
	c.cooldowns.Open(sessionStart, 0, nil) // empty the store, then refill it on this session's clock
	for name, session := range c.preemptedAt {
		if job := ssn.ClusterInfo.PodGroupInfos[jobID(name)]; job != nil {
			c.cooldowns.Record(job.PreemptCooldownKey(), atSession(session))
		}
	}
	ssn.LoadPreemptCooldowns(sessionStart)
	for name, session := range c.terminatingSince {
		job := ssn.ClusterInfo.PodGroupInfos[jobID(name)]
		if job == nil {
			continue
		}
		deleted := metav1.NewTime(atSession(session))
		for _, task := range job.GetAllPodsMap() {
			if task.Status == pod_status.Releasing {
				task.Pod.DeletionTimestamp = &deleted
				if node := ssn.ClusterInfo.Nodes[task.NodeName]; node != nil && node.PodInfos[task.UID] != nil {
					node.PodInfos[task.UID].Pod.DeletionTimestamp = &deleted
				}
			}
		}
	}
}

// advance records what the finished session did and turns it into the next session's cluster state.
// It returns the number of pods the session evicted.
func (c *cluster) advance(ssn *framework.Session, cycle int, sessionStart time.Time) int {
	open := c.cooldowns.Open(sessionStart, 24*time.Hour, func(string) bool { return true })
	for _, job := range c.topology.Jobs {
		preempted, found := open[ssn.ClusterInfo.PodGroupInfos[jobID(job.Name)].PreemptCooldownKey()]
		if !found {
			delete(c.preemptedAt, job.Name)
		} else if !preempted.Before(sessionStart) {
			c.preemptedAt[job.Name] = cycle
		}
	}

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

func gangState(ssn *framework.Session, name string) string {
	job := ssn.ClusterInfo.PodGroupInfos[jobID(name)]
	if job == nil {
		return "Absent"
	}
	counts := map[pod_status.PodStatus]int{}
	for _, task := range job.GetAllPodsMap() {
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
