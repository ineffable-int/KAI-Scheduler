// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"gopkg.in/h2non/gock.v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

func TestAllocateJobBindingReadiness(t *testing.T) {
	defer gock.Off()
	test_utils.InitTestingInfrastructure()
	failure := errors.New("readiness evaluation failed")
	for _, gpus := range []float64{1, 0.5} {
		for _, tc := range []struct {
			name   string
			hook   api.BindReadyFn
			err    error
			status pod_status.PodStatus
		}{
			{name: "no hooks allocate", status: pod_status.Allocated},
			{name: "ready allocates", hook: func(*pod_info.PodInfo, *node_info.NodeInfo) (bool, error) { return true, nil }, status: pod_status.Allocated},
			{name: "deferred pipelines", hook: func(*pod_info.PodInfo, *node_info.NodeInfo) (bool, error) { return false, nil }, status: pod_status.Pipelined},
			{name: "error preserves state", hook: func(*pod_info.PodInfo, *node_info.NodeInfo) (bool, error) { return false, failure }, err: failure, status: pod_status.Pending},
		} {
			t.Run(fmt.Sprintf("%g-gpus-%s", gpus, tc.name), func(t *testing.T) {
				ssn := test_utils.BuildSession(test_utils.TestTopologyBasic{
					Nodes:  map[string]nodes_fake.TestNodeBasic{"node0": {GPUs: 2}},
					Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 2, GPUOverQuotaWeight: 1}},
					Jobs:   []*jobs_fake.TestJobBasic{{Name: "job0", QueueName: "queue0", RequiredGPUsPerTask: gpus, Tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}}}},
					Mocks:  &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{}},
				}, gomock.NewController(t))
				var task *pod_info.PodInfo
				for _, candidate := range ssn.ClusterInfo.PodGroupInfos["job0"].GetAllPodsMap() {
					task = candidate
				}
				require.NotNil(t, task)
				if tc.hook != nil {
					ssn.AddBindReadyFn(tc.hook)
				}
				stmt := ssn.Statement()
				checkpoint := stmt.Checkpoint()
				success := common.AllocateJob(ssn, stmt, []*node_info.NodeInfo{ssn.ClusterInfo.Nodes["node0"]}, ssn.ClusterInfo.PodGroupInfos["job0"], podgroup_info.RealTaskAllocation)
				require.Equal(t, tc.err == nil, success)
				require.Equal(t, tc.status, task.Status)
				if tc.err != nil {
					require.Empty(t, task.NodeName)
					require.Empty(t, ssn.ClusterInfo.Nodes["node0"].PodInfos)
				} else {
					require.Equal(t, "node0", task.NodeName)
					require.Len(t, ssn.ClusterInfo.Nodes["node0"].PodInfos, 1)
				}
				require.NoError(t, stmt.Rollback(checkpoint))
				require.Equal(t, pod_status.Pending, task.Status)
				require.Empty(t, ssn.ClusterInfo.Nodes["node0"].PodInfos)
			})
		}
	}
}

// Upstream exercises this with a releasing pod's anti-affinity; here a readiness hook defers
// node0 directly, so the nodeavailability score must rank node1 above node0's extra order score.
func TestAllocateJobPrefersBindingReadyNode(t *testing.T) {
	defer gock.Off()
	test_utils.InitTestingInfrastructure()
	for _, gpus := range []float64{1, 0.5} {
		t.Run(fmt.Sprintf("%g-gpus", gpus), func(t *testing.T) {
			ssn := test_utils.BuildSession(test_utils.TestTopologyBasic{
				Nodes: map[string]nodes_fake.TestNodeBasic{
					"node0": {GPUs: 4},
					"node1": {GPUs: 4},
				},
				Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 8, GPUOverQuotaWeight: 1}},
				Jobs: []*jobs_fake.TestJobBasic{
					{Name: "incoming", QueueName: "queue0", Priority: constants.PriorityTrainNumber, RequiredGPUsPerTask: gpus, Tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}}},
				},
				Mocks: &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{}},
			}, gomock.NewController(t))
			ssn.AddBindReadyFn(func(_ *pod_info.PodInfo, node *node_info.NodeInfo) (bool, error) {
				return node.Name != "node0", nil
			})
			ssn.AddNodeOrderFn(func(_ *pod_info.PodInfo, node *node_info.NodeInfo) (float64, error) {
				if node.Name == "node0" {
					return 1, nil
				}
				return 0, nil
			})
			job := ssn.ClusterInfo.PodGroupInfos["incoming"]
			stmt := ssn.Statement()
			checkpoint := stmt.Checkpoint()
			require.True(t, common.AllocateJob(ssn, stmt, []*node_info.NodeInfo{ssn.ClusterInfo.Nodes["node0"], ssn.ClusterInfo.Nodes["node1"]}, job, podgroup_info.RealTaskAllocation))
			for _, task := range job.GetAllPodsMap() {
				require.Equal(t, "node1", task.NodeName)
				require.Equal(t, pod_status.Allocated, task.Status)
			}
			require.NoError(t, stmt.Rollback(checkpoint))
		})
	}
}
