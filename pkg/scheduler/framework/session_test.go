// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package framework

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	schedulingv1alpha2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v1alpha2"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/bindrequest_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	scheduler_cache "github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/cache"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
)

const benchmarkBindRequestCount = 5000

func TestSessionClearDropsRetainedReferences(t *testing.T) {
	ssn := &Session{
		ClusterInfo: &api.ClusterInfo{
			BindRequests: bindrequest_info.BindRequestMap{
				bindrequest_info.NewKey("namespace", "pod"): &bindrequest_info.BindRequestInfo{},
			},
			BindRequestsForDeletedNodes: []*bindrequest_info.BindRequestInfo{{}},
		},
		Config:               &conf.SchedulerConfiguration{},
		plugins:              map[string]Plugin{"plugin": nil},
		eventHandlers:        []*EventHandler{{}},
		TaskOrderFns:         []common_info.CompareFn{nil},
		PrePredicateFns:      []api.PrePredicateFn{nil},
		BindRequestMutateFns: []api.BindRequestMutateFn{nil},
	}
	ssn.k8sResourceStateCache.Store("resource", "state")

	ssn.clear()

	assert.Nil(t, ssn.ClusterInfo)
	assert.Nil(t, ssn.Config)
	assert.Nil(t, ssn.plugins)
	assert.Nil(t, ssn.eventHandlers)
	assert.Nil(t, ssn.TaskOrderFns)
	assert.Nil(t, ssn.PrePredicateFns)
	assert.Nil(t, ssn.BindRequestMutateFns)
	_, found := ssn.k8sResourceStateCache.Load("resource")
	assert.False(t, found)
}

func TestCloseSessionReleasesSnapshotReferencesWhileSessionIsLive(t *testing.T) {
	finalized := make(chan struct{})
	cacheMock := scheduler_cache.NewMockCache(gomock.NewController(t))
	cacheMock.EXPECT().WaitForWorkers(gomock.Any()).Times(1)
	ssn := newSessionWithFinalizedBindRequest(t, cacheMock, finalized)

	closeSession(ssn)

	requireFinalized(t, finalized, ssn)
}

// TestOpenSessionLoadsPreemptCooldowns: open windows carry over from the scheduler's in-memory store to the
// next session's jobs; ended windows and deleted jobs leave the store.
func TestOpenSessionLoadsPreemptCooldowns(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name      string
		cooldown  time.Duration
		preempted time.Duration // how long before now the job last preempted
		deleted   bool          // the job is no longer in the snapshot
		wantOpen  bool
	}{
		{name: "inside the window", cooldown: 3 * time.Minute, preempted: time.Minute, wantOpen: true},
		{name: "window elapsed", cooldown: 3 * time.Minute, preempted: 5 * time.Minute},
		{name: "cool-down disabled", cooldown: 0, preempted: time.Minute},
		{name: "job deleted", cooldown: 3 * time.Minute, preempted: time.Minute, deleted: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preempted := now.Add(-tt.preempted)
			job := podgroup_info.NewPodGroupInfo("job")
			job.NamespacedName = "ns/job"
			store := conf.NewPreemptCooldowns()
			store.Record(job.PreemptCooldownKey(), preempted)
			jobs := map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{"job": job}
			if tt.deleted {
				jobs = nil
			}
			cacheMock := scheduler_cache.NewMockCache(gomock.NewController(t))
			cacheMock.EXPECT().Snapshot().Return(&api.ClusterInfo{PodGroupInfos: jobs}, nil)

			ssn, err := openSession(cacheMock, "test",
				conf.SchedulerParams{PreemptCooldown: tt.cooldown, PreemptCooldowns: store}, nil)
			require.NoError(t, err)
			t.Cleanup(ssn.releaseNodeScoringPool)

			if tt.wantOpen {
				assert.Equal(t, preempted, *job.LastPreemptionTimestamp)
			} else {
				assert.Nil(t, job.LastPreemptionTimestamp)
			}
			// What the store still holds is what the next session would see.
			again := store.Open(now, 3*time.Minute, func(string) bool { return true })
			assert.Equal(t, tt.wantOpen, len(again) == 1)
		})
	}
}

// TestOpenSessionWithoutPreemptCooldownStore: sessions built without a store (tests, snapshot replays) keep no
// windows between sessions.
func TestOpenSessionWithoutPreemptCooldownStore(t *testing.T) {
	job := podgroup_info.NewPodGroupInfo("job")
	cacheMock := scheduler_cache.NewMockCache(gomock.NewController(t))
	cacheMock.EXPECT().Snapshot().Return(&api.ClusterInfo{
		PodGroupInfos: map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{"job": job},
	}, nil)

	ssn, err := openSession(cacheMock, "test", conf.SchedulerParams{PreemptCooldown: 3 * time.Minute}, nil)
	require.NoError(t, err)
	t.Cleanup(ssn.releaseNodeScoringPool)
	assert.Nil(t, job.LastPreemptionTimestamp)
}

func BenchmarkOpenCloseSessionWithLargeSnapshot(b *testing.B) {
	cacheMock := scheduler_cache.NewMockCache(gomock.NewController(b))
	cacheMock.EXPECT().Snapshot().AnyTimes().DoAndReturn(func() (*api.ClusterInfo, error) {
		return newClusterInfoWithBindRequests(benchmarkBindRequestCount), nil
	})
	cacheMock.EXPECT().WaitForWorkers(gomock.Any()).AnyTimes()

	runtime.GC()
	before := heapAlloc()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ssn, err := openSession(cacheMock, "benchmark", conf.SchedulerParams{}, nil)
		if err != nil {
			b.Fatal(err)
		}
		closeSession(ssn)
	}
	b.StopTimer()

	runtime.GC()
	after := heapAlloc()
	retained := int64(after) - int64(before)
	if retained < 0 {
		retained = 0
	}
	b.ReportMetric(benchmarkBindRequestCount, "bind_requests/op")
	b.ReportMetric(float64(retained)/float64(b.N), "retained_after_1_gc_B/op")
}

func newSessionWithFinalizedBindRequest(
	t *testing.T, cache scheduler_cache.Cache, finalized chan<- struct{},
) *Session {
	t.Helper()

	bindRequest := &schedulingv1alpha2.BindRequest{
		Spec: schedulingv1alpha2.BindRequestSpec{
			PodName: "pod",
		},
	}
	runtime.SetFinalizer(bindRequest, func(*schedulingv1alpha2.BindRequest) {
		close(finalized)
	})

	ssn := &Session{
		Cache: cache,
		ClusterInfo: &api.ClusterInfo{
			BindRequests: bindrequest_info.BindRequestMap{
				bindrequest_info.NewKey("namespace", "pod"): bindrequest_info.NewBindRequestInfo(bindRequest),
			},
			BindRequestsForDeletedNodes: []*bindrequest_info.BindRequestInfo{
				bindrequest_info.NewBindRequestInfo(bindRequest),
			},
		},
	}
	if err := ssn.InitNodeScoringPool(); err != nil {
		t.Fatalf("failed to initialize node scoring pool: %v", err)
	}
	return ssn
}

func requireFinalized(t *testing.T, finalized <-chan struct{}, keepAlive *Session) {
	t.Helper()

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()

	for {
		runtime.GC()
		select {
		case <-finalized:
			runtime.KeepAlive(keepAlive)
			return
		case <-deadline.C:
			runtime.KeepAlive(keepAlive)
			t.Fatal("snapshot bind request was still reachable after closeSession")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func heapAlloc() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func newClusterInfoWithBindRequests(count int) *api.ClusterInfo {
	clusterInfo := &api.ClusterInfo{
		BindRequests: make(bindrequest_info.BindRequestMap, count),
	}
	for i := 0; i < count; i++ {
		podName := "pod-" + strconv.Itoa(i)
		bindRequest := &schedulingv1alpha2.BindRequest{
			Spec: schedulingv1alpha2.BindRequestSpec{
				PodName: podName,
			},
		}
		clusterInfo.BindRequests[bindrequest_info.NewKey("namespace", podName)] =
			bindrequest_info.NewBindRequestInfo(bindRequest)
	}
	return clusterInfo
}
