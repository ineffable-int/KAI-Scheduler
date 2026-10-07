// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt_test

import (
	"fmt"
	"strings"
	"testing"

	. "go.uber.org/mock/gomock"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

// runPreemptAndCollectVictims runs preempt and returns the number of evicted or moved
// non-preemptor pods per original node, and the number of preemptor pods placed.
func runPreemptAndCollectVictims(t *testing.T, topo test_utils.TestTopologyBasic) (map[string]int, int) {
	controller := NewController(t)
	defer controller.Finish()
	ssn := test_utils.BuildSession(topo, controller)
	before := map[string]string{}
	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		for _, task := range job.GetAllPodsMap() {
			before[string(task.UID)] = task.NodeName
		}
	}
	preempt.New().Execute(ssn)

	victims := map[string]int{}
	placed := 0
	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		for _, task := range job.GetAllPodsMap() {
			if job.Name == "preemptor" {
				if task.Status == pod_status.Pipelined {
					placed++
				}
				continue
			}
			if task.Status == pod_status.Releasing || task.Status == pod_status.Pipelined {
				victims[before[string(task.UID)]]++
			}
		}
	}
	return victims, placed
}

// A gang pinned to the nodes of one group must only evict pods on those nodes, not pods
// on other nodes that the simulation evicted and then moved elsewhere.
func TestPreemptPinnedGangEvictsOnlyPinnedNodes(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	const groups, perGroup = 3, 8
	nodes := map[string]nodes_fake.TestNodeBasic{}
	var jobs []*jobs_fake.TestJobBasic
	age := 1000
	for g := 0; g < groups; g++ {
		for i := 0; i < perGroup; i++ {
			name := fmt.Sprintf("g%d-n%02d", g, i)
			nodes[name] = nodes_fake.TestNodeBasic{GPUs: 4}
			age -= 2
			jobs = append(jobs, &jobs_fake.TestJobBasic{
				Name: "victim-" + name, RequiredGPUsPerTask: 4, Priority: 25, QueueName: "q",
				JobAgeInMinutes: age,
				Tasks:           []*tasks_fake.TestTaskBasic{{NodeName: name, State: pod_status.Running}},
			})
		}
	}
	var gang []*tasks_fake.TestTaskBasic
	for i := 0; i < perGroup; i++ {
		gang = append(gang, &tasks_fake.TestTaskBasic{State: pod_status.Pending,
			NodeAffinityNames: []string{fmt.Sprintf("g0-n%02d", i)}})
	}
	jobs = append(jobs, &jobs_fake.TestJobBasic{Name: "preemptor", RequiredGPUsPerTask: 4, Priority: 75,
		QueueName: "q", Tasks: gang, JobAgeInMinutes: 1})

	victims, placed := runPreemptAndCollectVictims(t, test_utils.TestTopologyBasic{
		Name: "pinned gang", Nodes: nodes, Jobs: jobs,
		Queues: []test_utils.TestQueueBasic{{Name: "q", DeservedGPUs: 4 * groups * perGroup, GPUOverQuotaWeight: 1,
			MaxAllowedGPUs: -1}},
		Mocks: &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{
			NumberOfCacheEvictions: 1000, NumberOfPipelineActions: 1000}},
	})
	if placed != perGroup {
		t.Fatalf("preemptor placed %d of %d pods", placed, perGroup)
	}
	total := 0
	for node, n := range victims {
		total += n
		if !strings.HasPrefix(node, "g0-") {
			t.Errorf("victim on unpinned node %s", node)
		}
	}
	if total != perGroup {
		t.Errorf("expected %d victims, got %d", perGroup, total)
	}
}
