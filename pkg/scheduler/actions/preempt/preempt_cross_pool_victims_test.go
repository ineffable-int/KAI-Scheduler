// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt_test

import (
	"fmt"
	"sort"
	"testing"

	kaiv1alpha1 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/kai/v1alpha1"
	. "go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
// and the committed victim set contains pods that run on RTX nodes, which the gang can
// never use. Those pods are deleted for nothing.
//
// The test asserts the CORRECT behaviour: no pod on an RTX node is evicted. Cases that
// reproduce the bug FAIL on v0.18.5 (fork commit 41467076); that failure is the reproduction,
// and the unchanged table is the regression test for a fix (nothing to flip). Run
//
//	go test ./pkg/scheduler/actions/preempt/ -run TestPreemptGangDoesNotEvictOtherPool -v
//
// The scenario mirrors production: the scheduler runs with allowConsolidatingReclaim=true
// (the test session default) and the default generators (sg-nodelocalgreedy, sg-multinodegang).

const (
	acceleratorLabel = "cloud.google.com/gke-accelerator"
	subblockLabel    = "cloud.google.com/gce-topology-subblock"
	gb200            = "nvidia-gb200"
	rtx              = "nvidia-rtx-pro-6000"

	lowPriority    int32 = 25
	mediumPriority int32 = 50
	highPriority   int32 = 75
)

type crossPoolCase struct {
	name string
	// racks and nodesPerRack describe the GB200 pool (4 GPUs per node).
	racks, nodesPerRack int
	// rtxNodes and rtxGPUs describe the RTX pool. Every RTX node is full with single-GPU jobs
	// (like the leaderboard pods): rtxLowGPUs of them low priority (preemptible), the rest high
	// priority.
	rtxNodes, rtxGPUs, rtxLowGPUs int
	// gangNodes is the number of pods (one per GB200 node, 4 GPUs each) in the preemptor gang.
	gangNodes int
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
	// rtxJobsNewer makes the RTX jobs newer than the GB200 jobs, so they are tried first as
	// victims (KAI orders victims of equal priority newest first).
	rtxJobsNewer bool
}

func TestPreemptGangDoesNotEvictOtherPool(t *testing.T) {
	cases := []crossPoolCase{
		{
			name:  "node selector only, one rack",
			racks: 1, nodesPerRack: 4, rtxNodes: 4, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			rtxJobsNewer: true,
		},
		{
			name:  "node selector only, two racks",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			rtxJobsNewer: true,
		},
		{
			name:  "rack topology, two racks, one rack non-preemptible",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			withTopology: true, highPriorityRacks: []int{1}, rtxJobsNewer: true,
		},
		{
			name:  "rack topology, four racks, three non-preemptible",
			racks: 4, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			withTopology: true, highPriorityRacks: []int{1, 2, 3}, rtxJobsNewer: true,
		},
		{
			name:  "rack topology, 2-node GB200 victim gangs",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			withTopology: true, highPriorityRacks: []int{1}, rtxJobsNewer: true, gbJobNodes: 2,
		},
		{
			// Greedy node-local search locks onto rack 0 (lower priority victims are tried first),
			// which cannot fit the gang; the multi-node generator then has to find rack 1.
			name:  "rack topology, victim queue leads into a rack that cannot fit the gang",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			withTopology: true, blockedNodes: map[int]int{0: 1}, rackPriority: map[int]int32{0: 10},
			rtxJobsNewer: true,
		},
		{
			// What voyager-7 looks like: the leaderboard pods on the RTX nodes use CPUs only, so
			// every RTX GPU is idle. The capacity heuristic counts those idle GPUs as room for
			// the gang and picks RTX nodes whose victims free nothing the gang can use.
			name:  "CPU-only RTX pods leave RTX GPUs idle",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			withTopology: true, rtxJobsNewer: true, rtxCPUOnly: true,
		},
		{
			name:  "rack topology, CPU-only RTX pods (idle RTX GPUs), queue leads into a short rack",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 8, gangNodes: 4,
			withTopology: true, blockedNodes: map[int]int{0: 1}, rackPriority: map[int]int32{0: 10},
			rtxJobsNewer: true, rtxCPUOnly: true,
		},
		{
			name:  "rack topology, RTX jobs older than GB200 jobs",
			racks: 2, nodesPerRack: 4, rtxNodes: 8, rtxGPUs: 8, rtxLowGPUs: 4, gangNodes: 4,
			withTopology: true, highPriorityRacks: []int{1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			victimsByPool, placed, total := runCrossPoolScenario(t, tc)
			if placed != tc.gangNodes {
				t.Fatalf("preemptor placed %d of %d pods (victims: %v)", placed, tc.gangNodes, victimsByPool)
			}
			if rtxVictims := victimsByPool[rtx]; rtxVictims != 0 {
				t.Errorf("evicted %d pods on RTX nodes the gang cannot use (%d victims in total: %v)",
					rtxVictims, total, victimsByPool)
			}
		})
	}
}

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
				acceleratorLabel: gb200, subblockLabel: fmt.Sprintf("rack%d", r)}}
			poolOf[name] = gb200
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
	for n := 0; n < tc.rtxNodes; n++ {
		name := fmt.Sprintf("rtx-n%d", n)
		nodes[name] = nodes_fake.TestNodeBasic{GPUs: tc.rtxGPUs, CPUMillis: 64, CPUMemory: 1e12,
			Labels: map[string]string{acceleratorLabel: rtx}}
		poolOf[name] = rtx
		for g := 0; g < tc.rtxGPUs; g++ {
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
	root.AddPodSet(subgroup_info.NewPodSet("rack", int32(tc.gangNodes), constraint))
	var gang []*tasks_fake.TestTaskBasic
	for i := 0; i < tc.gangNodes; i++ {
		gang = append(gang, &tasks_fake.TestTaskBasic{
			State: pod_status.Pending, SubGroupName: "rack",
			NodeSelector: map[string]string{acceleratorLabel: gb200},
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
	for _, task := range ssn.ClusterInfo.PodGroupInfos["preemptor"].GetAllPodsMap() {
		if task.Status == pod_status.Pipelined {
			placed++
		}
	}
	pools := make([]string, 0, len(byPool))
	for p := range byPool {
		pools = append(pools, p)
	}
	sort.Strings(pools)
	t.Logf("%s: %d evictions %v, gang placed %d/%d", tc.name, len(evicted), byPool, placed, tc.gangNodes)
	return byPool, placed, len(evicted)
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
