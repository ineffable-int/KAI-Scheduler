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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf_util"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
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

// channelNodesTopology: each node has 4 GPUs and publishes one channel device, held by the
// claim of a releasing pod (holder-<node>-0) on that node. Every task of jobs names its own
// channel claim (<job>-<index>-channel), which starts unallocated.
func channelNodesTopology(t *testing.T, nodeNames []string, jobs ...*jobs_fake.TestJobBasic,
) test_utils.TestTopologyBasic {
	topology := test_utils.TestTopologyBasic{
		Name:   "channel nodes",
		Queues: []test_utils.TestQueueBasic{{Name: "q-1", DeservedGPUs: 16}},
		Nodes:  map[string]nodes_fake.TestNodeBasic{},
		TestDRAObjects: dra_fake.TestDRAObjects{
			DeviceClasses: []string{channelClass},
		},
		Mocks: &test_utils.TestMock{
			SchedulerConf: schedulerConfWithDRAArgs(t, map[string]string{
				dynamicresources.PipelineOverReleasingClaimsKey: "true",
			}),
			CacheRequirements: &test_utils.CacheMocking{NumberOfPipelineActions: 8},
		},
	}
	for _, nodeName := range nodeNames {
		holderJob, holderPod, holderClaim := "holder-"+nodeName, "holder-"+nodeName+"-0", "holder-"+nodeName+"-channel"
		topology.Nodes[nodeName] = nodes_fake.TestNodeBasic{GPUs: 4}
		topology.Jobs = append(topology.Jobs, &jobs_fake.TestJobBasic{
			Name: holderJob, Namespace: "test", QueueName: "q-1",
			Tasks: []*tasks_fake.TestTaskBasic{
				{State: pod_status.Releasing, NodeName: nodeName, ResourceClaimNames: []string{holderClaim}},
			},
		})
		topology.TestDRAObjects.ResourceSlices = append(topology.TestDRAObjects.ResourceSlices,
			&dra_fake.TestResourceSlice{Name: nodeName + "-channel", DeviceClassName: channelClass, NodeName: nodeName, Count: 1})
		topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims,
			&dra_fake.TestResourceClaim{
				Name: holderClaim, Namespace: "test", DeviceClassName: channelClass, Count: 1,
				ClaimStatus: &resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{Request: "request", Driver: "nvidia.com/gpu", Pool: nodeName, Device: "0"},
							},
						},
					},
					ReservedFor: []resourceapi.ResourceClaimConsumerReference{
						{Resource: "pods", Name: holderPod, UID: types.UID(holderPod)},
					},
				},
			})
	}
	for _, job := range jobs {
		for i, task := range job.Tasks {
			if task.ResourceClaimNames != nil {
				continue
			}
			claimName := fmt.Sprintf("%s-%d-channel", job.Name, i)
			task.ResourceClaimNames = []string{claimName}
			topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims,
				&dra_fake.TestResourceClaim{Name: claimName, Namespace: "test", DeviceClassName: channelClass, Count: 1})
		}
		topology.Jobs = append(topology.Jobs, job)
	}
	return topology
}

func pendingChannelJob(name string, gpus float64, tasks int) *jobs_fake.TestJobBasic {
	job := &jobs_fake.TestJobBasic{Name: name, Namespace: "test", QueueName: "q-1", RequiredGPUsPerTask: gpus}
	for range tasks {
		job.Tasks = append(job.Tasks, &tasks_fake.TestTaskBasic{State: pod_status.Pending})
	}
	return job
}

// pipelinedOn counts the tasks of the named jobs pipelined on each node.
func pipelinedOn(ssn *framework.Session, jobNames ...string) map[string]int {
	counts := map[string]int{}
	for _, jobName := range jobNames {
		job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", jobName)]
		for _, task := range job.GetAllPodsMap() {
			if task.Status == pod_status.Pipelined {
				counts[task.NodeName]++
			}
		}
	}
	return counts
}

// A node publishing one channel device, held by a releasing pod, takes one waiting DRA task:
// two jobs that each fit the node's GPUs must not both be pipelined onto the same channel.
func TestPipelineOverReleasingClaims_OneDRAWaiterPerNode(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	for _, gpus := range []float64{1, 2} {
		t.Run(fmt.Sprintf("separate jobs, %v GPUs each", gpus), func(t *testing.T) {
			topology := channelNodesTopology(t, []string{"node0"},
				pendingChannelJob("pending-a", gpus, 1), pendingChannelJob("pending-b", gpus, 1))
			ssn := setupTestSession(t, topology, controller)
			allocate.New().Execute(ssn)

			assert.Equal(t, map[string]int{"node0": 1}, pipelinedOn(ssn, "pending-a", "pending-b"))
		})
		t.Run(fmt.Sprintf("two-pod gang, %v GPUs each", gpus), func(t *testing.T) {
			topology := channelNodesTopology(t, []string{"node0"}, pendingChannelJob("gang", gpus, 2))
			ssn := setupTestSession(t, topology, controller)
			allocate.New().Execute(ssn)

			assert.LessOrEqual(t, pipelinedOn(ssn, "gang")["node0"], 1)
		})
	}
}

// The guard is per node: a gang can pipeline one pod onto each of two nodes.
func TestPipelineOverReleasingClaims_GangAcrossNodes(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	topology := channelNodesTopology(t, []string{"node0", "node1"}, pendingChannelJob("gang", 2, 2))
	ssn := setupTestSession(t, topology, controller)
	allocate.New().Execute(ssn)

	assert.Equal(t, map[string]int{"node0": 1, "node1": 1}, pipelinedOn(ssn, "gang"))
}

// While a DRA task waits on node0, other DRA tasks are rejected there, non-DRA tasks still
// fit, and undoing the waiter's pipeline (rollback or discard) frees the node again.
func TestPipelineOverReleasingClaims_WaiterLifecycle(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	plain := pendingChannelJob("plain", 1, 1)
	plain.Tasks[0].ResourceClaimNames = []string{}
	topology := channelNodesTopology(t, []string{"node0"},
		pendingChannelJob("waiter", 1, 1), pendingChannelJob("other", 1, 1), plain)

	for _, undo := range []string{"rollback", "discard"} {
		t.Run(undo, func(t *testing.T) {
			ssn := setupTestSession(t, topology, controller)
			node := ssn.ClusterInfo.Nodes["node0"]
			fits := func(jobName string) error {
				job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", jobName)]
				task := getTaskByStatus(t, ssn, jobName, pod_status.Pending)
				require.NoError(t, ssn.PrePredicateFn(task, job))
				return ssn.PredicateFn(task, job, node)
			}
			require.NoError(t, fits("waiter"))
			require.NoError(t, fits("other"))

			stmt := ssn.Statement()
			cp := stmt.Checkpoint()
			waiter := getTaskByStatus(t, ssn, "waiter", pod_status.Pending)
			require.NoError(t, stmt.Pipeline(waiter, "node0", false))
			claim, err := ssn.InternalK8sPlugins().SessionDRAManager.ResourceClaims().Get("test", "waiter-0-channel")
			require.NoError(t, err)
			require.Nil(t, claim.Status.Allocation)

			assert.Error(t, fits("other"), "a second DRA task must not wait on the same node")
			assert.NoError(t, fits("plain"), "non-DRA tasks are not affected")

			if undo == "rollback" {
				require.NoError(t, stmt.Rollback(cp))
			} else {
				stmt.Discard()
			}
			assert.Equal(t, pod_status.Pending, waiter.Status)
			assert.NoError(t, fits("other"))
		})
	}
}

// Relaxed filtering only covers exclusive, node-local claims of a single pod. A claim shared
// with another pod, or a device that is not local to the node, keeps the strict filter.
func TestPipelineOverReleasingClaims_UnsupportedShapesStayStrict(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	t.Run("claim shared by two pods", func(t *testing.T) {
		shared := pendingChannelJob("shared", 1, 2)
		for _, task := range shared.Tasks {
			task.ResourceClaimNames = []string{"shared-channel"}
		}
		topology := channelNodesTopology(t, []string{"node0"}, shared)
		topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims,
			&dra_fake.TestResourceClaim{Name: "shared-channel", Namespace: "test", DeviceClassName: channelClass, Count: 1})
		ssn := setupTestSession(t, topology, controller)
		job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "shared")]
		task := getTaskByStatus(t, ssn, "shared", pod_status.Pending)
		require.NoError(t, ssn.PrePredicateFn(task, job))
		assert.Error(t, ssn.PredicateFn(task, job, ssn.ClusterInfo.Nodes["node0"]))
	})

	// The allocator does not count admin-access results as allocated, so the device of a
	// releasing admin-access claim may also be allocated to a running claim. The retry must
	// not free it.
	t.Run("releasing admin-access claim on a device in use", func(t *testing.T) {
		topology := releasingClaimTopology(t, pod_status.Releasing, true, 0)
		holderResult := &topology.TestDRAObjects.ResourceClaims[0].ClaimStatus.Allocation.Devices.Results[0]
		holderResult.AdminAccess = ptr.To(true)
		topology.Jobs = append(topology.Jobs, &jobs_fake.TestJobBasic{
			Name: "user", Namespace: "test", QueueName: "q-1",
			Tasks: []*tasks_fake.TestTaskBasic{
				{State: pod_status.Running, NodeName: "node0", ResourceClaimNames: []string{"user-channel"}},
			},
		})
		sameDevice := resourceapi.DeviceRequestAllocationResult{
			Request: "request", Driver: holderResult.Driver, Pool: holderResult.Pool, Device: holderResult.Device,
		}
		topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims,
			&dra_fake.TestResourceClaim{
				Name: "user-channel", Namespace: "test", DeviceClassName: channelClass, Count: 1,
				ClaimStatus: &resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{sameDevice},
						},
					},
					ReservedFor: []resourceapi.ResourceClaimConsumerReference{
						{Resource: "pods", Name: "user-0", UID: "user-0"},
					},
				},
			})
		ssn := setupTestSession(t, topology, controller)
		job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "pending")]
		task := getTaskByStatus(t, ssn, "pending", pod_status.Pending)
		require.NoError(t, ssn.PrePredicateFn(task, job))
		assert.Error(t, ssn.PredicateFn(task, job, ssn.ClusterInfo.Nodes["node0"]))
	})

	t.Run("device not local to the node", func(t *testing.T) {
		topology := releasingClaimTopology(t, pod_status.Releasing, true, 0)
		topology.TestDRAObjects.ResourceSlices = []*dra_fake.TestResourceSlice{
			{Name: "cluster-channel", DeviceClassName: channelClass, AllNodes: true, PoolName: "cluster", Count: 1},
		}
		topology.TestDRAObjects.ResourceClaims[0].ClaimStatus.Allocation.Devices.Results[0].Pool = "cluster"
		ssn := setupTestSession(t, topology, controller)
		job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "pending")]
		task := getTaskByStatus(t, ssn, "pending", pod_status.Pending)
		require.NoError(t, ssn.PrePredicateFn(task, job))
		assert.Error(t, ssn.PredicateFn(task, job, ssn.ClusterInfo.Nodes["node0"]))
	})
}
