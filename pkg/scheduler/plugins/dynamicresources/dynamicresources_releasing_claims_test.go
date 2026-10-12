// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package dynamicresources_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	. "go.uber.org/mock/gomock"
	resourceapi "k8s.io/api/resource/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf_util"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/dynamicresources"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/dra_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

const channelClass = "channel.example.com"

// schedulerConfWithDRAArgs is the default scheduler configuration with the given arguments
// on the dynamicresources plugin.
func schedulerConfWithDRAArgs(t *testing.T, args map[string]string) *conf.SchedulerConfiguration {
	defaults, err := conf_util.GetDefaultSchedulerConf()
	require.NoError(t, err)
	config := &conf.SchedulerConfiguration{Actions: defaults.Actions, Tiers: defaults.Tiers}
	for i := range config.Tiers {
		for j := range config.Tiers[i].Plugins {
			if config.Tiers[i].Plugins[j].Name == "dynamicresources" {
				config.Tiers[i].Plugins[j].Arguments = args
			}
		}
	}
	return config
}

// releasingClaimTopology: node0 has one channel device, held by the claim of holder-0. A
// pending job needs a channel device and nothing else, so its only obstacle is that claim.
func releasingClaimTopology(t *testing.T, holderState pod_status.PodStatus, pipelineOverReleasing bool,
	binds int) test_utils.TestTopologyBasic {
	return test_utils.TestTopologyBasic{
		Name:   fmt.Sprintf("holder %s, pipelineOverReleasingClaims=%t", holderState, pipelineOverReleasing),
		Queues: []test_utils.TestQueueBasic{{Name: "q-1", DeservedGPUs: 1}},
		Jobs: []*jobs_fake.TestJobBasic{
			{
				Name: "holder", Namespace: "test", QueueName: "q-1",
				Tasks: []*tasks_fake.TestTaskBasic{
					{State: holderState, NodeName: "node0", ResourceClaimNames: []string{"holder-channel"}},
				},
			},
			{
				Name: "pending", Namespace: "test", QueueName: "q-1",
				Tasks: []*tasks_fake.TestTaskBasic{
					{State: pod_status.Pending, ResourceClaimNames: []string{"pending-channel"}},
				},
			},
		},
		Nodes: map[string]nodes_fake.TestNodeBasic{"node0": {GPUs: 1}},
		TestDRAObjects: dra_fake.TestDRAObjects{
			DeviceClasses: []string{channelClass},
			ResourceSlices: []*dra_fake.TestResourceSlice{
				{Name: "node0-channel", DeviceClassName: channelClass, NodeName: "node0", Count: 1},
			},
			ResourceClaims: []*dra_fake.TestResourceClaim{
				{
					Name: "holder-channel", Namespace: "test", DeviceClassName: channelClass, Count: 1,
					ClaimStatus: &resourceapi.ResourceClaimStatus{
						Allocation: &resourceapi.AllocationResult{
							Devices: resourceapi.DeviceAllocationResult{
								Results: []resourceapi.DeviceRequestAllocationResult{
									{Request: "request", Driver: "nvidia.com/gpu", Pool: "node0", Device: "0"},
								},
							},
						},
						ReservedFor: []resourceapi.ResourceClaimConsumerReference{
							{Resource: "pods", Name: "holder-0", UID: "holder-0"},
						},
					},
				},
				{Name: "pending-channel", Namespace: "test", DeviceClassName: channelClass, Count: 1},
			},
		},
		Mocks: &test_utils.TestMock{
			SchedulerConf: schedulerConfWithDRAArgs(t, map[string]string{
				dynamicresources.PipelineOverReleasingClaimsKey: fmt.Sprintf("%t", pipelineOverReleasing),
			}),
			CacheRequirements: &test_utils.CacheMocking{NumberOfCacheBinds: binds, NumberOfPipelineActions: 1},
		},
	}
}

// The filter accepts a node whose only DRA obstacle is a claim held by releasing pods only
// when pipelineOverReleasingClaims is on, and bind readiness then refuses to bind there.
func TestPipelineOverReleasingClaims_FilterAndBindReadiness(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	for _, holderState := range []pod_status.PodStatus{pod_status.Releasing, pod_status.Running} {
		for _, on := range []bool{false, true} {
			topology := releasingClaimTopology(t, holderState, on, 0)
			t.Run(topology.Name, func(t *testing.T) {
				ssn := setupTestSession(t, topology, controller)
				job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "pending")]
				task := getTaskByStatus(t, ssn, "pending", pod_status.Pending)
				node := ssn.ClusterInfo.Nodes["node0"]

				require.NoError(t, ssn.PrePredicateFn(task, job))
				err := ssn.PredicateFn(task, job, node)
				fits := on && holderState == pod_status.Releasing
				if fits {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}

				ready, err := ssn.IsTaskReadyForBinding(task, node)
				assert.NoError(t, err)
				assert.Equal(t, !on, ready, "bind readiness is only checked with the argument on")
			})
		}
	}
}

// End to end through allocate: with the argument on, the job is pipelined onto the node whose
// device is still held by a releasing pod, its claim is not allocated, and nothing is bound.
// With it off, the job stays pending (and preempt would look for more victims).
func TestPipelineOverReleasingClaims_Allocate(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	for _, on := range []bool{false, true} {
		topology := releasingClaimTopology(t, pod_status.Releasing, on, 0)
		t.Run(topology.Name, func(t *testing.T) {
			ssn := setupTestSession(t, topology, controller)
			allocate.New().Execute(ssn)

			job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "pending")]
			for _, task := range job.GetAllPodsMap() {
				if on {
					assert.Equal(t, pod_status.Pipelined, task.Status)
					assert.Equal(t, "node0", task.NodeName)
				} else {
					assert.Equal(t, pod_status.Pending, task.Status)
				}
			}

			claim, err := ssn.InternalK8sPlugins().SessionDRAManager.ResourceClaims().Get("test", "pending-channel")
			require.NoError(t, err)
			assert.Nil(t, claim.Status.Allocation, "a claim pipelined over releasing devices stays unallocated")
		})
	}
}

// Once the releasing pod is gone, the device is free and the task binds as before.
func TestPipelineOverReleasingClaims_BindsWhenDeviceIsFree(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	topology := releasingClaimTopology(t, pod_status.Releasing, true, 1)
	topology.Jobs = topology.Jobs[1:]
	topology.TestDRAObjects.ResourceClaims = topology.TestDRAObjects.ResourceClaims[1:]

	ssn := setupTestSession(t, topology, controller)
	task := getTaskByStatus(t, ssn, "pending", pod_status.Pending)
	ready, err := ssn.IsTaskReadyForBinding(task, ssn.ClusterInfo.Nodes["node0"])
	require.NoError(t, err)
	assert.True(t, ready)

	allocate.New().Execute(ssn)
	assert.Equal(t, pod_status.Allocated, task.Status)
	claim, err := ssn.InternalK8sPlugins().SessionDRAManager.ResourceClaims().Get("test", "pending-channel")
	require.NoError(t, err)
	assert.NotNil(t, claim.Status.Allocation)
}

// Fractional GPU and GPU memory tasks take the GPU sharing allocation path, which must also
// pipeline, not bind, while the task's claim needs a device held by a releasing pod.
func TestPipelineOverReleasingClaims_AllocateSharedGPU(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	for name, configure := range map[string]func(*jobs_fake.TestJobBasic){
		"fractional GPU": func(job *jobs_fake.TestJobBasic) { job.RequiredGPUsPerTask = 0.5 },
		"GPU memory":     func(job *jobs_fake.TestJobBasic) { job.RequiredGpuMemory = 40 },
	} {
		topology := releasingClaimTopology(t, pod_status.Releasing, true, 0)
		configure(topology.Jobs[1])
		t.Run(name, func(t *testing.T) {
			ssn := setupTestSession(t, topology, controller)
			task := getTaskByStatus(t, ssn, "pending", pod_status.Pending)
			require.True(t, task.IsSharedGPURequest())
			allocate.New().Execute(ssn)

			assert.Equal(t, pod_status.Pipelined, task.Status)
			assert.Equal(t, "node0", task.NodeName)
			claim, err := ssn.InternalK8sPlugins().SessionDRAManager.ResourceClaims().Get("test", "pending-channel")
			require.NoError(t, err)
			assert.Nil(t, claim.Status.Allocation, "a claim pipelined over releasing devices stays unallocated")
		})
	}
}
