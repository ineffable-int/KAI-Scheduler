// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package consolidation_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	. "go.uber.org/mock/gomock"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/consolidation"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf_util"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/priority"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

// schedulerConfWithPriorityArgs is the default scheduler configuration with the given
// arguments on the priority plugin.
func schedulerConfWithPriorityArgs(t *testing.T, args map[string]string) *conf.SchedulerConfiguration {
	defaults, err := conf_util.GetDefaultSchedulerConf()
	require.NoError(t, err)
	config := &conf.SchedulerConfiguration{Actions: defaults.Actions, Tiers: defaults.Tiers}
	for i := range config.Tiers {
		for j := range config.Tiers[i].Plugins {
			if config.Tiers[i].Plugins[j].Name == "priority" {
				config.Tiers[i].Plugins[j].Arguments = args
			}
		}
	}
	return config
}

// Two 4-GPU nodes, each half used by a 2-GPU job of priority victimPriority. A pending 4-GPU
// job of priority 25 fits only if one of them moves next to the other.
func priorityTopology(t *testing.T, victimPriority int32, respect bool, evictions int) test_utils.TestTopologyBasic {
	running := func(name, node string) *jobs_fake.TestJobBasic {
		return &jobs_fake.TestJobBasic{
			Name: name, RequiredGPUsPerTask: 2, Priority: victimPriority, QueueName: "queue0",
			Tasks: []*tasks_fake.TestTaskBasic{{NodeName: node, State: pod_status.Running}},
		}
	}
	return test_utils.TestTopologyBasic{
		Name: fmt.Sprintf("victims at %d, consolidationRespectPriority=%t", victimPriority, respect),
		Jobs: []*jobs_fake.TestJobBasic{
			running("running-job-0", "node0"),
			running("running-job-1", "node1"),
			{Name: "pending-job", RequiredGPUsPerTask: 4, Priority: 25, QueueName: "queue0",
				Tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}}},
		},
		Nodes:  map[string]nodes_fake.TestNodeBasic{"node0": {GPUs: 4}, "node1": {GPUs: 4}},
		Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 8}},
		Mocks: &test_utils.TestMock{
			SchedulerConf: schedulerConfWithPriorityArgs(t, map[string]string{
				priority.ConsolidationRespectPriorityKey: fmt.Sprintf("%t", respect),
			}),
			CacheRequirements: &test_utils.CacheMocking{
				NumberOfCacheEvictions: evictions, NumberOfPipelineActions: 2 * evictions},
		},
	}
}

// With consolidationRespectPriority a priority-25 job cannot move a job of priority 50 or 25
// to make room, but can move one of priority 10. Without it, it moves any of them.
func TestConsolidationRespectPriority(t *testing.T) {
	for _, respect := range []bool{false, true} {
		for _, victim := range []int32{50, 25, 10} {
			consolidates := !respect || victim < 25
			evictions := 0
			if consolidates {
				evictions = 1
			}
			topology := priorityTopology(t, victim, respect, evictions)
			t.Run(topology.Name, func(t *testing.T) {
				test_utils.InitTestingInfrastructure()
				controller := NewController(t)
				defer controller.Finish()
				ssn := test_utils.BuildSession(topology, controller)

				consolidation.New().Execute(ssn)

				want := pod_status.Pending
				if consolidates {
					want = pod_status.Pipelined
				}
				job := ssn.ClusterInfo.PodGroupInfos[common_info.PodGroupID("pending-job")]
				for _, task := range job.GetAllPodsMap() {
					require.Equal(t, want, task.Status, task.Name)
				}
			})
		}
	}
}
