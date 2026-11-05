// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common_test

import (
	"testing"

	. "go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

const poolLabel = "pool"

var (
	gbSelector  = map[string]string{poolLabel: "gb"}
	rtxSelector = map[string]string{poolLabel: "rtx"}
	poolTaint   = v1.Taint{Key: "dedicated", Value: "rtx", Effect: v1.TaintEffectNoSchedule}
)

func TestNodesUsableByJob(t *testing.T) {
	tests := []struct {
		name  string
		tasks []*tasks_fake.TestTaskBasic
		// mutate changes the session before the call: taints, tolerations, cordon, conditions.
		mutate func(ssn *framework.Session)
		want   []string
	}{
		{
			name:  "no node constraints: every node",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}},
			want:  []string{"gb-0", "gb-1", "rtx-0", "rtx-1"},
		},
		{
			name:  "node selector",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending, NodeSelector: gbSelector}},
			want:  []string{"gb-0", "gb-1"},
		},
		{
			name:  "required node affinity",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending, NodeAffinityNames: []string{"gb-1", "rtx-0"}}},
			want:  []string{"gb-1", "rtx-0"},
		},
		{
			name: "tasks with different selectors: union",
			tasks: []*tasks_fake.TestTaskBasic{
				{State: pod_status.Pending, NodeSelector: gbSelector},
				{State: pod_status.Pending, NodeSelector: gbSelector},
				{State: pod_status.Pending, NodeAffinityNames: []string{"rtx-1"}},
			},
			want: []string{"gb-0", "gb-1", "rtx-1"},
		},
		{
			name:  "running tasks of the job do not count",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending, NodeSelector: gbSelector}, {State: pod_status.Running, NodeName: "rtx-0", NodeSelector: rtxSelector}},
			want:  []string{"gb-0", "gb-1"},
		},
		{
			name:  "untolerated NoSchedule taint",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}},
			mutate: func(ssn *framework.Session) {
				taintPool(ssn, "rtx", poolTaint)
			},
			want: []string{"gb-0", "gb-1"},
		},
		{
			name:  "tolerated taint",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}},
			mutate: func(ssn *framework.Session) {
				taintPool(ssn, "rtx", poolTaint)
				tolerate(ssn, v1.Toleration{Key: "dedicated", Operator: v1.TolerationOpExists})
			},
			want: []string{"gb-0", "gb-1", "rtx-0", "rtx-1"},
		},
		{
			name:  "PreferNoSchedule taint does not exclude",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}},
			mutate: func(ssn *framework.Session) {
				taintPool(ssn, "rtx", v1.Taint{Key: "dedicated", Value: "rtx", Effect: v1.TaintEffectPreferNoSchedule})
			},
			want: []string{"gb-0", "gb-1", "rtx-0", "rtx-1"},
		},
		{
			name:  "cordoned node",
			tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending, NodeSelector: gbSelector}},
			mutate: func(ssn *framework.Session) {
				ssn.ClusterInfo.Nodes["gb-0"].Node.Spec.Unschedulable = true
			},
			want: []string{"gb-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssn := buildUsableNodesSession(t, tt.tasks)
			if tt.mutate != nil {
				tt.mutate(ssn)
			}
			got := common.NodesUsableByJob(ssn, ssn.ClusterInfo.PodGroupInfos["preemptor"], nil)
			if !got.Equal(sets.New(tt.want...)) {
				t.Errorf("NodesUsableByJob() = %v, want %v", sets.List(got), tt.want)
			}
		})
	}
}

func TestJobHasTaskOnNodes(t *testing.T) {
	ssn := buildUsableNodesSession(t, []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}})
	nodes := sets.New("gb-0")
	for name, want := range map[string]bool{"victim-gb-0": true, "victim-rtx-0": false, "victim-both": true} {
		if got := common.JobHasTaskOnNodes(ssn.ClusterInfo.PodGroupInfos[common_info.PodGroupID(name)], nodes); got != want {
			t.Errorf("JobHasTaskOnNodes(%s) = %v, want %v", name, got, want)
		}
	}
}

// The counters FilterVictimsToUsableNodes reports are what its temporary [victim-filter] log line
// prints.
func TestFilterVictimsToUsableNodes(t *testing.T) {
	ssn := buildUsableNodesSession(t, []*tasks_fake.TestTaskBasic{{State: pod_status.Pending, NodeSelector: gbSelector}})
	candidates := map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{}
	for _, name := range []common_info.PodGroupID{"victim-gb-0", "victim-rtx-0", "victim-both"} {
		candidates[name] = ssn.ClusterInfo.PodGroupInfos[name]
	}
	stats := common.FilterVictimsToUsableNodes(ssn, ssn.ClusterInfo.PodGroupInfos["preemptor"], candidates,
		framework.Preempt)

	want := common.VictimFilterStats{CandidateJobs: 3, UsableNodes: 2, ExcludedJobs: 1, ExcludedPods: 1,
		ExcludedNodes: []string{"rtx-0"}}
	if !equality.Semantic.DeepEqual(stats, want) {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
	if got := sets.KeySet(candidates); !got.Equal(sets.New[common_info.PodGroupID]("victim-gb-0", "victim-both")) {
		t.Errorf("kept victims %v", sets.List(got))
	}
}

func TestNoTasksToAllocateMeansNoRestriction(t *testing.T) {
	ssn := buildUsableNodesSession(t, []*tasks_fake.TestTaskBasic{{State: pod_status.Running, NodeName: "gb-1"}})
	usable := common.NodesUsableByJob(ssn, ssn.ClusterInfo.PodGroupInfos["preemptor"], nil)
	if usable != nil {
		t.Fatalf("NodesUsableByJob() = %v, want nil", sets.List(usable))
	}
	if !common.JobHasTaskOnNodes(ssn.ClusterInfo.PodGroupInfos["victim-rtx-0"], usable) {
		t.Errorf("JobHasTaskOnNodes(victim-rtx-0, nil) = false, want true")
	}
}

func buildUsableNodesSession(t *testing.T, preemptorTasks []*tasks_fake.TestTaskBasic) *framework.Session {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	t.Cleanup(controller.Finish)
	nodes := map[string]nodes_fake.TestNodeBasic{}
	for _, name := range []string{"gb-0", "gb-1"} {
		nodes[name] = nodes_fake.TestNodeBasic{GPUs: 4, Labels: gbSelector}
	}
	for _, name := range []string{"rtx-0", "rtx-1"} {
		nodes[name] = nodes_fake.TestNodeBasic{GPUs: 4, Labels: rtxSelector}
	}
	running := func(nodeNames ...string) []*tasks_fake.TestTaskBasic {
		var tasks []*tasks_fake.TestTaskBasic
		for _, n := range nodeNames {
			tasks = append(tasks, &tasks_fake.TestTaskBasic{NodeName: n, State: pod_status.Running})
		}
		return tasks
	}
	jobs := []*jobs_fake.TestJobBasic{
		{Name: "preemptor", RequiredGPUsPerTask: 1, Priority: 50, QueueName: "queue0", Tasks: preemptorTasks},
		{Name: "victim-gb-0", RequiredGPUsPerTask: 1, Priority: 10, QueueName: "queue0", Tasks: running("gb-0")},
		{Name: "victim-rtx-0", RequiredGPUsPerTask: 1, Priority: 10, QueueName: "queue0", Tasks: running("rtx-0")},
		{Name: "victim-both", RequiredGPUsPerTask: 1, Priority: 10, QueueName: "queue0", Tasks: running("rtx-1", "gb-0")},
	}
	return test_utils.BuildSession(test_utils.TestTopologyBasic{
		Name: t.Name(), Nodes: nodes, Jobs: jobs,
		Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 100}},
		Mocks:  &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{}},
	}, controller)
}

func taintPool(ssn *framework.Session, pool string, taint v1.Taint) {
	for _, node := range ssn.ClusterInfo.Nodes {
		if node.Node.Labels[poolLabel] == pool {
			node.Node.Spec.Taints = append(node.Node.Spec.Taints, taint)
		}
	}
}

func tolerate(ssn *framework.Session, toleration v1.Toleration) {
	for _, task := range ssn.ClusterInfo.PodGroupInfos["preemptor"].GetAllPodsMap() {
		task.Pod.Spec.Tolerations = append(task.Pod.Spec.Tolerations, toleration)
	}
}
