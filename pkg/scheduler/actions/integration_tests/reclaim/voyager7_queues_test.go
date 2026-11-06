// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

// Scenario tests for voyager-7's two KAI queues (ineffable-int/voyager#4579):
// default-queue, where every fly job runs, and offline-evals, which needs
// GB200s. Each scenario runs full scheduling rounds at voyager-7's real size
// under two configurations, ours and Michal's alternative, and checks what
// happens to the evals and to default-queue's jobs.

package reclaim

import (
	"fmt"
	"sort"
	"testing"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/stalegangeviction"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
	"go.uber.org/mock/gomock"
)

const (
	v7DefaultQueue = "default-queue"
	v7EvalsQueue   = "offline-evals"
	v7GB200Nodes   = 234 // 4 GPUs each
	v7RTXNodes     = 16  // 8 GPUs each

	// fly's PriorityClass values.
	flyLow      = 25
	flyMedium   = 50
	flyCritical = 125
)

type v7Config struct {
	name             string
	defaultGPUQuota  float64
	evalsOverQuotaWt float64
}

var (
	// #4579: 90% of the GB200s (842) less the evals' 8; evals weigh 1/20.
	v7Ours = v7Config{"ours (834, evals weight 0.05)", 834, 0.05}
	// Michal's suggestion on #4579: default-queue around 600, no weight for evals.
	v7Michal = v7Config{"michal (600, evals weight 0)", 600, 0}
	// What voyager-7 runs before #4579: default-queue's quota is effectively unbounded.
	v7Before = v7Config{"before #4579 (1000000, evals weight 0.05)", 1_000_000, 0.05}
)

func gbName(i int) string  { return fmt.Sprintf("gb-%03d", i) }
func rtxName(i int) string { return fmt.Sprintf("rtx-%02d", i) }

// v7Cluster is a voyager-7-shaped cluster with gbUp GB200 nodes Ready.
type v7Cluster struct {
	cfg  v7Config
	gbUp int
	jobs []*jobs_fake.TestJobBasic
}

func (c *v7Cluster) gbNodes() []string {
	names := make([]string, c.gbUp)
	for i := range names {
		names[i] = gbName(i)
	}
	return names
}

// defaultJob adds a one-pod default-queue job; node "" leaves it pending.
func (c *v7Cluster) defaultJob(name string, gpus int, priority int32, node string, affinity []string) {
	state := pod_status.Pending
	if node != "" {
		state = pod_status.Running
	}
	c.jobs = append(c.jobs, &jobs_fake.TestJobBasic{
		Name:                name,
		Namespace:           "fly-user",
		RequiredGPUsPerTask: float64(gpus),
		Priority:            priority,
		QueueName:           v7DefaultQueue,
		Tasks: []*tasks_fake.TestTaskBasic{{
			State: state, NodeName: node, NodeAffinityNames: affinity,
		}},
	})
}

// evalJob adds a pending offline-evals gang of `nodes` whole GB200 nodes.
func (c *v7Cluster) evalJob(name string, nodes int) {
	tasks := make([]*tasks_fake.TestTaskBasic, nodes)
	for i := range tasks {
		tasks[i] = &tasks_fake.TestTaskBasic{State: pod_status.Pending, NodeAffinityNames: c.gbNodes()}
	}
	c.jobs = append(c.jobs, &jobs_fake.TestJobBasic{
		Name:                name,
		Namespace:           "leaderboard",
		RequiredGPUsPerTask: 4,
		Priority:            flyLow,
		QueueName:           v7EvalsQueue,
		Tasks:               tasks,
	})
}

func (c *v7Cluster) topology() test_utils.TestTopologyBasic {
	nodes := map[string]nodes_fake.TestNodeBasic{}
	for i := 0; i < c.gbUp; i++ {
		nodes[gbName(i)] = nodes_fake.TestNodeBasic{GPUs: 4}
	}
	for i := 0; i < v7RTXNodes; i++ {
		nodes[rtxName(i)] = nodes_fake.TestNodeBasic{GPUs: 8}
	}
	lots := 1_000_000
	return test_utils.TestTopologyBasic{
		Name:  c.cfg.name,
		Jobs:  c.jobs,
		Nodes: nodes,
		Queues: []test_utils.TestQueueBasic{
			{Name: v7DefaultQueue, DeservedGPUs: c.cfg.defaultGPUQuota, GPUOverQuotaWeight: 1},
			{Name: v7EvalsQueue, DeservedGPUs: 8, GPUOverQuotaWeight: c.cfg.evalsOverQuotaWt},
		},
		Mocks: &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{
			NumberOfCacheBinds: lots, NumberOfCacheEvictions: lots, NumberOfPipelineActions: lots,
		}},
	}
}

// v7Outcome is what the rounds left behind, plus every eviction they made.
type v7Outcome struct {
	defaultGPUs, evalGPUs    int
	evalsRunning, evalsTotal int
	evictedByPriority        map[int32]int
	evictionsPerRound        []int
	pendingDefault           []string
	evalNodes                []string
}

func (o v7Outcome) String() string {
	return fmt.Sprintf("default-queue %d GPUs, offline-evals %d GPUs (%d/%d eval jobs running), "+
		"evictions by priority %v, evictions per round %v, eval nodes %v",
		o.defaultGPUs, o.evalGPUs, o.evalsRunning, o.evalsTotal, o.evictedByPriority, o.evictionsPerRound, o.evalNodes)
}

// run runs `rounds` scheduling cycles with voyager-7's actions (consolidation
// is disabled there). Like the cluster, an evicted pod comes back pending.
func (c *v7Cluster) run(t *testing.T, ctrl *gomock.Controller, rounds int) v7Outcome {
	t.Helper()
	actions := []framework.Action{allocate.New(), reclaim.New(), preempt.New(), stalegangeviction.New()}
	out := v7Outcome{evictedByPriority: map[int32]int{}}
	topology := c.topology()
	for round := 0; round < rounds; round++ {
		ssn := test_utils.BuildSession(topology, ctrl)
		for _, action := range actions {
			action.Execute(ssn)
		}
		evicted := 0
		for _, job := range c.jobs {
			pg := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID(job.Namespace, job.Name)]
			jobEvicted := false
			for i, task := range job.Tasks {
				pod := pg.GetAllPodsMap()[common_info.PodID(fmt.Sprintf("%s-%d", job.Name, i))]
				switch pod.Status {
				case pod_status.Releasing, pod_status.Pipelined:
					// Releasing: evicted. Pipelined from Running: evicted and moved
					// to another node in the same cycle. Either restarts the pod.
					if task.State == pod_status.Running {
						jobEvicted = true
					}
					task.State, task.NodeName = pod_status.Pending, ""
				case pod_status.Binding, pod_status.Running:
					task.State, task.NodeName = pod_status.Running, pod.NodeName
				default:
					task.State, task.NodeName = pod.Status, pod.NodeName
				}
			}
			if jobEvicted {
				evicted++
				out.evictedByPriority[job.Priority]++
			}
		}
		out.evictionsPerRound = append(out.evictionsPerRound, evicted)
	}
	for _, job := range c.jobs {
		running := 0
		for _, task := range job.Tasks {
			if task.State == pod_status.Running {
				running++
			}
		}
		gpus := running * int(job.RequiredGPUsPerTask)
		if job.QueueName == v7EvalsQueue {
			for _, task := range job.Tasks {
				if task.State == pod_status.Running {
					out.evalNodes = append(out.evalNodes, task.NodeName)
				}
			}
			out.evalGPUs += gpus
			out.evalsTotal++
			if running == len(job.Tasks) {
				out.evalsRunning++
			}
		} else {
			out.defaultGPUs += gpus
			if running == 0 {
				out.pendingDefault = append(out.pendingDefault, job.Name)
			}
		}
	}
	sort.Strings(out.pendingDefault)
	return out
}

func v7Run(t *testing.T, cfg v7Config, setup func(c *v7Cluster), rounds int) v7Outcome {
	t.Helper()
	test_utils.InitTestingInfrastructure()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	c := &v7Cluster{cfg: cfg, gbUp: v7GB200Nodes}
	setup(c)
	out := c.run(t, ctrl, rounds)
	t.Logf("%s: %s", cfg.name, out)
	return out
}

// fillDefault puts a running whole-node default job on every Ready node, the
// GB200s first at `gbPriority`, plus `pending` more GB200 jobs waiting, so
// default-queue wants more than the cluster has.
func fillDefault(c *v7Cluster, gbPriority func(i int) int32, rtx bool, pending int) {
	for i := 0; i < c.gbUp; i++ {
		c.defaultJob(fmt.Sprintf("gb-job-%03d", i), 4, gbPriority(i), gbName(i), nil)
	}
	if rtx {
		for i := 0; i < v7RTXNodes; i++ {
			c.defaultJob(fmt.Sprintf("rtx-job-%02d", i), 8, flyLow, rtxName(i), nil)
		}
	}
	for i := 0; i < pending; i++ {
		c.defaultJob(fmt.Sprintf("gb-pending-%03d", i), 4, flyLow, "", c.gbNodes())
	}
}

func lowAndMedium(i int) int32 {
	if i%2 == 0 {
		return flyMedium
	}
	return flyLow
}

func allLow(int) int32 { return flyLow }

func (o v7Outcome) evictions() int {
	total := 0
	for _, n := range o.evictionsPerRound {
		total += n
	}
	return total
}

// settled fails unless the first round made every eviction.
func (o v7Outcome) settled(t *testing.T) {
	t.Helper()
	for _, n := range o.evictionsPerRound[1:] {
		if n != 0 {
			t.Errorf("still evicting after the first round: %s", o)
		}
	}
}

func (o v7Outcome) onGB200s(t *testing.T) {
	t.Helper()
	for _, node := range o.evalNodes {
		if node[:3] != "gb-" {
			t.Errorf("eval on %s, not a GB200: %s", node, o)
		}
	}
}

// A full cluster: default-queue holds every GPU and wants more. One two-node
// eval arrives and should reclaim exactly two nodes, the low-priority ones,
// and the cluster should then stay put.
func TestVoyager7FullClusterEvalReclaims(t *testing.T) {
	for _, cfg := range []v7Config{v7Ours, v7Michal} {
		t.Run(cfg.name, func(t *testing.T) {
			out := v7Run(t, cfg, func(c *v7Cluster) {
				fillDefault(c, lowAndMedium, true, 20)
				c.evalJob("eval-0", 2)
			}, 8)
			if out.evalsRunning != 1 || out.evictions() != 2 || out.evictedByPriority[flyLow] != 2 {
				t.Errorf("want the eval running at the cost of two fly-low jobs: %s", out)
			}
			out.onGB200s(t)
			out.settled(t)
		})
	}
}

// Control: with default-queue's quota as it is before #4579, the same eval
// never gets in, because default-queue is never over its quota.
func TestVoyager7BeforeEvalStarves(t *testing.T) {
	out := v7Run(t, v7Before, func(c *v7Cluster) {
		fillDefault(c, lowAndMedium, true, 20)
		c.evalJob("eval-0", 2)
	}, 8)
	if out.evalsRunning != 0 || out.evalGPUs != 0 {
		t.Errorf("expected the eval to starve: %s", out)
	}
}

// Only 90% of the GB200s are up (211 of 234 nodes), all held by default-queue.
// With the RTXs busy too, and with them idle, the eval still gets its 8 GPUs.
func TestVoyager7NinetyPercentOfGB200sUp(t *testing.T) {
	for _, rtx := range []bool{true, false} {
		for _, cfg := range []v7Config{v7Ours, v7Michal} {
			t.Run(fmt.Sprintf("%s rtx busy=%v", cfg.name, rtx), func(t *testing.T) {
				out := v7Run(t, cfg, func(c *v7Cluster) {
					c.gbUp = 211
					fillDefault(c, allLow, rtx, 20)
					c.evalJob("eval-0", 2)
				}, 8)
				if out.evalsRunning != 1 || out.evictions() != 2 {
					t.Errorf("want the eval running at the cost of two jobs: %s", out)
				}
				out.onGB200s(t)
				out.settled(t)
			})
		}
	}
}

// default-queue sits exactly at 834 GPUs, all on GB200s, but its 1-GPU jobs
// leave a free GPU on 102 nodes and no free node. A two-node eval needs whole
// nodes. default-queue is not over quota under ours, but KAI still frees two
// nodes by moving 8 one-GPU jobs into the scattered free GPUs, so
// default-queue keeps all 834 GPUs.
func TestVoyager7FragmentedAtQuota(t *testing.T) {
	for _, cfg := range []v7Config{v7Ours, v7Michal} {
		t.Run(cfg.name, func(t *testing.T) {
			out := v7Run(t, cfg, func(c *v7Cluster) {
				for i := 0; i < v7GB200Nodes; i++ {
					used := 4
					if i < 102 {
						used = 3
					}
					for g := 0; g < used; g++ {
						c.defaultJob(fmt.Sprintf("gb-job-%03d-%d", i, g), 1, flyLow, gbName(i), nil)
					}
				}
				c.evalJob("eval-0", 2)
			}, 8)
			if out.evalsRunning != 1 || out.defaultGPUs != 834 || out.evictions() != 8 {
				t.Errorf("want the eval running and 8 one-GPU jobs moved: %s", out)
			}
			out.onGB200s(t)
			out.settled(t)
		})
	}
}

// Critical (non-preemptible) jobs: the eval reclaims only preemptible jobs,
// never a critical one.
func TestVoyager7CriticalNeverEvicted(t *testing.T) {
	for _, cfg := range []v7Config{v7Ours, v7Michal} {
		t.Run(cfg.name, func(t *testing.T) {
			out := v7Run(t, cfg, func(c *v7Cluster) {
				fillDefault(c, func(i int) int32 {
					if i < 140 {
						return flyCritical
					}
					return flyLow
				}, true, 20)
				c.evalJob("eval-0", 2)
			}, 8)
			if out.evalsRunning != 1 {
				t.Errorf("eval not running: %s", out)
			}
			if out.evictedByPriority[flyCritical] != 0 {
				t.Errorf("critical job evicted: %s", out)
			}
			out.settled(t)
		})
	}
}

// KAI admits a non-preemptible job only while its queue's non-preemptible
// GPUs stay within the queue's quota. default-queue already runs 600 GPUs of
// critical jobs and a 4-GPU critical job arrives, with free GB200s: a quota of
// 600 turns it away.
func TestVoyager7CriticalHeadroom(t *testing.T) {
	for _, tc := range []struct {
		cfg     v7Config
		pending bool
	}{{v7Ours, false}, {v7Michal, true}} {
		cfg := tc.cfg
		t.Run(cfg.name, func(t *testing.T) {
			out := v7Run(t, cfg, func(c *v7Cluster) {
				for i := 0; i < 150; i++ {
					c.defaultJob(fmt.Sprintf("crit-%03d", i), 4, flyCritical, gbName(i), nil)
				}
				c.defaultJob("crit-new", 4, flyCritical, "", c.gbNodes())
			}, 4)
			if pending := len(out.pendingDefault) > 0; pending != tc.pending {
				t.Errorf("new critical job pending = %v, want %v: %s", pending, tc.pending, out)
			}
		})
	}
}

// Thirty two-node evals arrive at a full cluster: how much do the evals take,
// how many default jobs are evicted, and does it settle?
// Ours lets the evals take a second job from their weight; Michal's keeps them
// to their quota.
func TestVoyager7ManyEvalsOnFullCluster(t *testing.T) {
	for _, tc := range []struct {
		cfg      v7Config
		evalGPUs int
	}{{v7Ours, 16}, {v7Michal, 8}} {
		cfg := tc.cfg
		t.Run(cfg.name, func(t *testing.T) {
			out := v7Run(t, cfg, func(c *v7Cluster) {
				fillDefault(c, allLow, true, 20)
				for i := 0; i < 30; i++ {
					c.evalJob(fmt.Sprintf("eval-%02d", i), 2)
				}
			}, 10)
			if out.evalGPUs != tc.evalGPUs || out.evictions() != tc.evalGPUs/4 {
				t.Errorf("want evals on %d GPUs, evicting one job per node: %s", tc.evalGPUs, out)
			}
			out.onGB200s(t)
			out.settled(t)
		})
	}
}

// Evals borrow an idle cluster, then default-queue's demand returns (on
// GB200s only, so the RTXs stay idle). default-queue takes the GPUs back down
// to the evals' quota, plus one job's worth of weight under ours.
func TestVoyager7EvalsBorrowThenDefaultReturns(t *testing.T) {
	for _, tc := range []struct {
		cfg      v7Config
		evalGPUs int
	}{{v7Ours, 16}, {v7Michal, 8}} {
		cfg := tc.cfg
		t.Run(cfg.name, func(t *testing.T) {
			test_utils.InitTestingInfrastructure()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			c := &v7Cluster{cfg: cfg, gbUp: v7GB200Nodes}
			for i := 0; i < 30; i++ {
				c.evalJob(fmt.Sprintf("eval-%02d", i), 2)
			}
			before := c.run(t, ctrl, 3)
			if before.evalGPUs != 240 {
				t.Errorf("want all 30 evals on the idle cluster: %s", before)
			}
			for i := 0; i < 300; i++ {
				c.defaultJob(fmt.Sprintf("back-%03d", i), 4, flyLow, "", c.gbNodes())
			}
			after := c.run(t, ctrl, 10)
			t.Logf("%s, default back: %s", cfg.name, after)
			if after.evalGPUs != tc.evalGPUs || after.defaultGPUs != 936-tc.evalGPUs {
				t.Errorf("want evals down to %d GPUs and default-queue on the rest: %s", tc.evalGPUs, after)
			}
			after.settled(t)
		})
	}
}
