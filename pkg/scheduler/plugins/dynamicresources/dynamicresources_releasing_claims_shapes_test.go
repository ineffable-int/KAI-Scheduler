// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package dynamicresources_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	k8sfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/consolidation"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/eviction_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/cache"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/dynamicresources"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/dra_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

// openPipelineOverReleasing opens a fresh dynamicresources plugin with the argument on over
// ssn's cluster, after a test has changed ClusterInfo.
func openPipelineOverReleasing(ssn *framework.Session) *framework.Session {
	draSession := &framework.Session{ClusterInfo: ssn.ClusterInfo, Cache: ssn.Cache, SchedulerParams: ssn.SchedulerParams}
	dynamicresources.New(framework.PluginArguments{dynamicresources.PipelineOverReleasingClaimsKey: "true"}).
		OnSessionOpen(draSession)
	return draSession
}

// A claim reserved for a live pod the session does not track (another scheduler's pod, not
// bound to a node yet) is held, not released: only a pod missing from the cluster is deleted.
func TestPipelineOverReleasingClaims_LiveUntrackedConsumerHolds(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	topology := releasingClaimTopology(t, pod_status.Releasing, true, 0)
	topology.Jobs = topology.Jobs[1:] // the holder is not a session pod
	ssn := setupTestSession(t, topology, gomock.NewController(t))
	ssn.ClusterInfo.PodPhases["holder-0"] = v1.PodPending

	draSession := openPipelineOverReleasing(ssn)
	task := getTaskByStatus(t, draSession, "pending", pod_status.Pending)
	job := draSession.ClusterInfo.PodGroupInfos[task.Job]
	require.NoError(t, draSession.PrePredicateFn(task, job))
	require.Error(t, draSession.PredicateFn(task, job, draSession.ClusterInfo.Nodes["node0"]))
}

// editSlices edits the ResourceSlices in the fake informer store the session's DRA manager reads.
func editSlices(t *testing.T, ssn *framework.Session, edit func(*resourceapi.ResourceSlice)) {
	t.Helper()
	store := ssn.Cache.KubeInformerFactory().Resource().V1().ResourceSlices().Informer().GetStore()
	for _, object := range store.List() {
		slice := object.(*resourceapi.ResourceSlice).DeepCopy()
		edit(slice)
		require.NoError(t, store.Update(slice))
	}
}

func selectClaimKind(t *testing.T, ssn *framework.Session, name, kind string) {
	t.Helper()
	tracker := ssn.InternalK8sPlugins().SessionDRAManager.ResourceClaims()
	claim, err := tracker.Get("test", name)
	require.NoError(t, err)
	claim = claim.DeepCopy()
	claim.Spec.Devices.Requests[0].Exactly.Selectors = []resourceapi.DeviceSelector{{
		CEL: &resourceapi.CELDeviceSelector{Expression: fmt.Sprintf("device.attributes[\"nvidia.com/gpu\"].kind == %q", kind)},
	}}
	require.NoError(t, tracker.AssumeClaimAfterAPICall(claim))
}

// A waiter on node0's channel device must not keep a task that needs a different, free device
// off node0: rejecting it there sends preempt, reclaim and consolidation to evict elsewhere.
func TestPipelineOverReleasingClaims_WaiterDoesNotBlockOtherDevices(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	for _, action := range []framework.ActionType{framework.Preempt, framework.Reclaim, framework.Consolidation} {
		for _, on := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/argument=%t", action, on), func(t *testing.T) {
				waiter := pendingChannelJob("waiter", 1, 1)
				waiter.Priority = 75
				incoming := pendingChannelJob("incoming", 2, 1)
				incoming.Priority = 50
				topology := channelNodesTopology(t, []string{"node0"}, waiter, incoming)
				topology.Mocks.SchedulerConf = schedulerConfWithDRAArgs(t, map[string]string{
					dynamicresources.PipelineOverReleasingClaimsKey: fmt.Sprint(on),
				})
				topology.Mocks.CacheRequirements.NumberOfCacheBinds = 2
				topology.Nodes["node1"] = nodes_fake.TestNodeBasic{GPUs: 2}
				topology.TestDRAObjects.ResourceSlices = append(topology.TestDRAObjects.ResourceSlices,
					&dra_fake.TestResourceSlice{Name: "node0-free", NodeName: "node0", PoolName: "node0-free", Count: 1},
					&dra_fake.TestResourceSlice{Name: "node1-free", NodeName: "node1", PoolName: "node1-free", Count: 1})
				victimQueue := "q-1"
				if action == framework.Reclaim {
					victimQueue = "borrower"
					topology.Queues = append(topology.Queues, test_utils.TestQueueBasic{Name: victimQueue, GPUOverQuotaWeight: 1})
				}
				topology.Jobs = append(topology.Jobs, &jobs_fake.TestJobBasic{
					Name: "victim", Namespace: "test", QueueName: victimQueue, Priority: 10, RequiredGPUsPerTask: 2,
					Tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Running, NodeName: "node1"}},
				})
				ssn := setupTestSession(t, topology, gomock.NewController(t))
				var evicted []string
				ssn.Cache.(*cache.MockCache).EXPECT().Evict(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(pod *v1.Pod, _ *podgroup_info.PodGroupInfo, _ eviction_info.EvictionMetadata, _ string) error {
						evicted = append(evicted, pod.Name)
						return nil
					}).AnyTimes()
				editSlices(t, ssn, func(slice *resourceapi.ResourceSlice) {
					slice.Spec.AllNodes = nil
					kind := "channel"
					if slice.Name != "node0-channel" {
						kind = "free"
					}
					for i := range slice.Spec.Devices {
						slice.Spec.Devices[i].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"kind": {StringValue: ptr.To(kind)}}
					}
				})
				selectClaimKind(t, ssn, "waiter-0-channel", "channel")
				selectClaimKind(t, ssn, "incoming-0-channel", "free")
				allocate.New().Execute(ssn)
				switch action {
				case framework.Preempt:
					preempt.New().Execute(ssn)
				case framework.Reclaim:
					reclaim.New().Execute(ssn)
				case framework.Consolidation:
					consolidation.New().Execute(ssn)
				}
				var incomingTask *pod_info.PodInfo
				for _, task := range ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "incoming")].GetAllPodsMap() {
					incomingTask = task
				}
				var victimStatus pod_status.PodStatus
				for _, task := range ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "victim")].GetAllPodsMap() {
					victimStatus = task.Status
				}
				t.Logf("incoming=%s on %s; victim=%s; actual evictions=%v", incomingTask.Status, incomingTask.NodeName, victimStatus, evicted)
				require.Empty(t, evicted, "this placement requires no eviction")
				require.Equal(t, pod_status.Running, victimStatus, "node0 has idle GPUs and a free device of a different kind")
			})
		}
	}
}

// A pod that also uses an existing claim held only by releasing pods needs that claim's
// device. The relaxed check must not free it for the pod's other claim.
func TestPipelineOverReleasingClaims_ReusedReleasingClaimStaysStrict(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("argument=%t", on), func(t *testing.T) {
			topology := releasingClaimTopology(t, pod_status.Releasing, on, 0)
			topology.Jobs[1].Tasks[0].ResourceClaimNames = []string{"holder-channel", "pending-channel"}
			ssn := setupTestSession(t, topology, gomock.NewController(t))
			job := ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "pending")]
			task := getTaskByStatus(t, ssn, "pending", pod_status.Pending)
			node := ssn.ClusterInfo.Nodes["node0"]
			require.NoError(t, ssn.PrePredicateFn(task, job))
			err := ssn.PredicateFn(task, job, node)
			t.Logf("one device, two exclusive claims needed by the same pod: filter error=%v", err)
			if err == nil {
				stmt := ssn.Statement()
				cp := stmt.Checkpoint()
				require.NoError(t, stmt.Pipeline(task, "node0", false))
				claim, getErr := ssn.InternalK8sPlugins().SessionDRAManager.ResourceClaims().Get("test", "holder-channel")
				require.NoError(t, getErr)
				require.Len(t, claim.Status.ReservedFor, 2)
				ready, checkErr := ssn.IsTaskReadyForBinding(task, node)
				require.NoError(t, checkErr)
				require.False(t, ready)
				require.NoError(t, stmt.Rollback(cp))
				require.Equal(t, pod_status.Pending, task.Status)
			}
			require.Error(t, err, "the pod still needs holder-channel, so that device cannot satisfy pending-channel")
		})
	}
}

// Partitionable devices on different nodes can draw on one shared counter. Freeing the
// releasing pod's device frees its counter units for every such device, so a device that
// consumes counters is not exclusive to its node and stays strict.
func TestPipelineOverReleasingClaims_SharedCounterDevicesStayStrict(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, k8sfeatures.DRAPartitionableDevices, true)
	gang := pendingChannelJob("gang", 1, 2)
	topology := channelNodesTopology(t, []string{"node0", "node1"}, gang)
	topology.Jobs = append(topology.Jobs[:1], topology.Jobs[2:]...)
	topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims[:1], topology.TestDRAObjects.ResourceClaims[2:]...)
	topology.TestDRAObjects.ResourceClaims[0].ClaimStatus.Allocation.Devices.Results[0].Pool = "cluster"
	topology.TestDRAObjects.ResourceSlices = []*dra_fake.TestResourceSlice{
		{Name: "local-node0", NodeName: "node0", PoolName: "cluster", Count: 1},
		{Name: "local-node1", NodeName: "node1", PoolName: "cluster", Count: 1},
		{Name: "counters", AllNodes: true, PoolName: "cluster"},
	}
	ssn := setupTestSession(t, topology, gomock.NewController(t))
	edit := func(slice *resourceapi.ResourceSlice) {
		slice.Spec.AllNodes = nil
		counter := map[string]resourceapi.Counter{"unit": {Value: resource.MustParse("1")}}
		if slice.Name == "counters" {
			slice.Spec.NodeName = nil
			slice.Spec.AllNodes = ptr.To(true)
			slice.Spec.SharedCounters = []resourceapi.CounterSet{{Name: "shared", Counters: counter}}
			return
		}
		if slice.Name == "local-node1" {
			slice.Spec.Devices[0].Name = "1"
		}
		slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{{CounterSet: "shared", Counters: counter}}
	}
	editSlices(t, ssn, edit)
	for _, node := range ssn.ClusterInfo.Nodes {
		task := getTaskByStatus(t, ssn, "gang", pod_status.Pending)
		ready, err := ssn.IsTaskReadyForBinding(task, node)
		require.NoError(t, err)
		require.False(t, ready, "the holder consumes the counter on either node")
	}
	allocate.New().Execute(ssn)
	counts := pipelinedOn(ssn, "gang")
	t.Logf("waiters for one shared counter unit: %v", counts)
	// Even after the holder disappears, the gang cannot bind both pods.
	topology.Jobs = topology.Jobs[1:]
	topology.TestDRAObjects.ResourceClaims = topology.TestDRAObjects.ResourceClaims[1:]
	freeSession := setupTestSession(t, topology, gomock.NewController(t))
	editSlices(t, freeSession, edit)
	allocate.New().Execute(freeSession)
	for _, task := range freeSession.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "gang")].GetAllPodsMap() {
		require.Equal(t, pod_status.Pending, task.Status)
	}
	t.Log("after the holder disappears: both gang pods are still Pending")
	require.LessOrEqual(t, counts["node0"]+counts["node1"], 1, "different DeviceIDs still consume the same exclusive backing capacity")
}

// Slices of an older pool generation linger while a driver republishes. Only the newest
// generation, which the allocator uses, may make a device local to a node.
func TestPipelineOverReleasingClaims_OnlyCurrentSlicesCountAsLocal(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := gomock.NewController(t)
	gang := pendingChannelJob("gang", 1, 2)
	topology := channelNodesTopology(t, []string{"node0", "node1"}, gang)
	// A pool moved from node0 to node1, then became accessible from all nodes.
	// Old generations coexist with the complete current generation during cleanup.
	topology.Jobs = append(topology.Jobs[:1], topology.Jobs[2:]...)
	topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims[:1], topology.TestDRAObjects.ResourceClaims[2:]...)
	topology.TestDRAObjects.ResourceClaims[0].ClaimStatus.Allocation.Devices.Results[0].Pool = "cluster"
	topology.TestDRAObjects.ResourceSlices = []*dra_fake.TestResourceSlice{
		{Name: "old-node0", NodeName: "node0", PoolName: "cluster", Count: 1},
		{Name: "old-node1", NodeName: "node1", PoolName: "cluster", Count: 1},
		{Name: "current-global", AllNodes: true, PoolName: "cluster", Count: 1},
	}
	ssn := setupTestSession(t, topology, controller)
	editSlices(t, ssn, func(slice *resourceapi.ResourceSlice) {
		slice.Spec.Pool.ResourceSliceCount = 1
		slice.Spec.AllNodes = nil
		switch slice.Name {
		case "old-node0":
			slice.Spec.Pool.Generation = 1
		case "old-node1":
			slice.Spec.Pool.Generation = 2
		case "current-global":
			slice.Spec.Pool.Generation = 3
			slice.Spec.NodeName = nil
			slice.Spec.AllNodes = ptr.To(true)
		}
	})
	allocate.New().Execute(ssn)
	counts := pipelinedOn(ssn, "gang")
	t.Logf("pipelined pods for the single cluster/0 device: %v", counts)
	for _, task := range ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "gang")].GetAllPodsMap() {
		if task.NodeName == "" {
			continue
		}
		ready, err := ssn.IsTaskReadyForBinding(task, ssn.ClusterInfo.Nodes[task.NodeName])
		require.NoError(t, err)
		require.False(t, ready)
	}
	require.LessOrEqual(t, counts["node0"]+counts["node1"], 1, "a global exclusive device cannot be promised twice")
}

// A claim that needs a device held by a releasing pod and a free device cannot be pipelined:
// its claim stays unallocated while it waits, so nothing reserves the free device, and a
// later, lower-priority pod could take it and leave the waiter unable to bind.
func TestPipelineOverReleasingClaims_MixedClaimStaysStrict(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	topology := releasingClaimTopology(t, pod_status.Releasing, true, 1)
	topology.TestDRAObjects.ResourceSlices[0].Count = 2 // device 0 held by the holder, device 1 free
	topology.TestDRAObjects.ResourceClaims[1].Count = 2 // the pending pod needs both
	topology.Jobs[1].Priority = 100
	topology.Jobs = append(topology.Jobs, &jobs_fake.TestJobBasic{
		Name: "later", Namespace: "test", QueueName: "q-1", Priority: 10,
		Tasks: []*tasks_fake.TestTaskBasic{{State: pod_status.Pending, ResourceClaimNames: []string{"later-channel"}}},
	})
	topology.TestDRAObjects.ResourceClaims = append(topology.TestDRAObjects.ResourceClaims,
		&dra_fake.TestResourceClaim{Name: "later-channel", Namespace: "test", DeviceClassName: channelClass, Count: 1})
	ssn := setupTestSession(t, topology, gomock.NewController(t))

	allocate.New().Execute(ssn)

	for _, task := range ssn.ClusterInfo.PodGroupInfos[common_info.NewPodGroupID("test", "pending")].GetAllPodsMap() {
		require.Equal(t, pod_status.Pending, task.Status, "a waiter for one held and one free device cannot keep the free one")
	}
}
