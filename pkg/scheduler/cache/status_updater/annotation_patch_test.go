// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package status_updater

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	faketesting "k8s.io/client-go/testing"

	kubeaischedfake "github.com/kai-scheduler/KAI-scheduler/pkg/apis/client/clientset/versioned/fake"
	fakeschedulingv2alpha2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/client/clientset/versioned/typed/scheduling/v2alpha2/fake"
	enginev2alpha2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v2alpha2"
	commonconstants "github.com/kai-scheduler/KAI-scheduler/pkg/common/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
)

const (
	testStartTime = "2020-01-01T00:00:00Z"
	testStaleTime = "2020-01-01T00:01:00Z"
)

func notStaleJob(pg *enginev2alpha2.PodGroup) *podgroup_info.PodGroupInfo {
	job := podgroup_info.NewPodGroupInfo("pg")
	job.SetPodGroup(pg)
	job.StalenessInfo.TimeStamp = nil
	job.StalenessInfo.Stale = false
	return job
}

func testPodGroup(annotations map[string]string) *enginev2alpha2.PodGroup {
	return &enginev2alpha2.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name: "pg", Namespace: "ns", UID: "uid", ResourceVersion: "1", Annotations: annotations,
	}}
}

func waitUntil(timeout time.Duration, condition func() bool) bool {
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if condition() {
			return true
		}
	}
	return condition()
}

// An applied update removed the stale annotation, but the snapshot is older and still has it.
// The overlay must remove the key from the snapshot, not set it to "", or the next cycle
// sends a second removal of a key the server no longer has.
func TestSyncRemovesAnnotationMissingFromAppliedUpdate(t *testing.T) {
	su := New(nil, nil, nil, 1, false, "")
	key := su.keyForPodGroupPayload("pg", "ns", "uid")
	live := testPodGroup(map[string]string{
		commonconstants.LastStartTimeStamp:     testStartTime,
		commonconstants.StalePodgroupTimeStamp: testStaleTime,
	})

	job := notStaleJob(live.DeepCopy())
	patch, err := su.updatePodGroupAnnotations(job)
	if err != nil || patch == nil {
		t.Fatalf("expected a patch removing the stale annotation, got %s, %v", patch, err)
	}
	su.appliedPodGroupUpdates.Store(key, &inflightUpdate{object: job.PodGroup, patchData: patch})

	snapshot := live.DeepCopy()
	su.SyncPodGroupsWithPendingUpdates([]*enginev2alpha2.PodGroup{snapshot})
	if value, found := snapshot.Annotations[commonconstants.StalePodgroupTimeStamp]; found {
		t.Errorf("stale annotation still in snapshot after sync: %q", value)
	}
	if patch, _ := su.updatePodGroupAnnotations(notStaleJob(snapshot)); patch != nil {
		t.Errorf("expected no further patch, got %s", patch)
	}
}

// An update adding the stale annotation is still queued when the next cycle removes it again.
// The removal must apply to the live object, which never had the key.
func TestRemovalOfUnsentAnnotationApplies(t *testing.T) {
	kaiClient := kubeaischedfake.NewSimpleClientset()
	su := New(nil, kaiClient, nil, 1, false, "")
	key := su.keyForPodGroupPayload("pg", "ns", "uid")
	live := testPodGroup(map[string]string{commonconstants.LastStartTimeStamp: testStartTime})
	if _, err := kaiClient.SchedulingV2alpha2().PodGroups("ns").Create(
		context.Background(), live, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var patchCalls atomic.Int32
	kaiClient.SchedulingV2alpha2().(*fakeschedulingv2alpha2.FakeSchedulingV2alpha2).PrependReactor(
		"patch", "podgroups", func(faketesting.Action) (bool, runtime.Object, error) {
			patchCalls.Add(1)
			return false, nil, nil
		})

	snapshot := live.DeepCopy()
	su.SyncPodGroupsWithPendingUpdates([]*enginev2alpha2.PodGroup{snapshot})
	staleJob := notStaleJob(snapshot)
	staleTime, _ := time.Parse(time.RFC3339, testStaleTime)
	staleJob.StalenessInfo.TimeStamp = &staleTime
	addPatch, _ := su.updatePodGroupAnnotations(staleJob)
	su.inFlightPodGroups.Store(key, &inflightUpdate{object: staleJob.PodGroup, patchData: addPatch})

	snapshot = live.DeepCopy()
	su.SyncPodGroupsWithPendingUpdates([]*enginev2alpha2.PodGroup{snapshot})
	job := notStaleJob(snapshot)
	removePatch, _ := su.updatePodGroupAnnotations(job)
	if removePatch == nil {
		t.Fatal("expected a patch removing the queued stale annotation")
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	su.Run(stopCh)
	su.pushToUpdateQueue(&updatePayload{key: key, objectType: podGroupType},
		&inflightUpdate{object: job.PodGroup, patchData: removePatch})

	if !waitUntil(2*time.Second, func() bool { _, found := su.inFlightPodGroups.Load(key); return !found }) {
		t.Fatalf("update still in flight after %d patch calls", patchCalls.Load())
	}
	got, err := kaiClient.SchedulingV2alpha2().PodGroups("ns").Get(context.Background(), "pg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := got.Annotations[commonconstants.StalePodgroupTimeStamp]; found {
		t.Errorf("stale annotation present on live object")
	}
	if got.Annotations[commonconstants.LastStartTimeStamp] != testStartTime {
		t.Errorf("unrelated annotation changed: %v", got.Annotations)
	}
}

func runFailingPatch(t *testing.T, patchErr error, wait time.Duration) (int32, bool) {
	kaiClient := kubeaischedfake.NewSimpleClientset()
	su := New(nil, kaiClient, nil, 1, false, "")
	key := su.keyForPodGroupPayload("pg", "ns", "uid")
	var patchCalls atomic.Int32
	kaiClient.SchedulingV2alpha2().(*fakeschedulingv2alpha2.FakeSchedulingV2alpha2).PrependReactor(
		"patch", "podgroups", func(faketesting.Action) (bool, runtime.Object, error) {
			patchCalls.Add(1)
			return true, nil, patchErr
		})

	stopCh := make(chan struct{})
	defer close(stopCh)
	su.Run(stopCh)
	su.pushToUpdateQueue(&updatePayload{key: key, objectType: podGroupType},
		&inflightUpdate{object: testPodGroup(nil), patchData: []byte(`{}`)})
	time.Sleep(wait)
	_, inFlight := su.inFlightPodGroups.Load(key)
	return patchCalls.Load(), inFlight
}

func TestInvalidPatchIsNotRetried(t *testing.T) {
	invalid := apierrors.NewInvalid(schema.GroupKind{Group: "scheduling.run.ai", Kind: "PodGroup"}, "pg",
		field.ErrorList{field.Invalid(field.NewPath("metadata"), "", "test")})
	calls, inFlight := runFailingPatch(t, invalid, 300*time.Millisecond)
	if calls != 1 {
		t.Errorf("expected 1 patch call, got %d", calls)
	}
	if inFlight {
		t.Errorf("update still in flight after an invalid patch")
	}
}

func TestFailedPatchIsRetriedWithBackoff(t *testing.T) {
	calls, inFlight := runFailingPatch(t, errors.New("test"), 300*time.Millisecond)
	if calls < 2 || calls > 4 {
		t.Errorf("expected 2-4 patch calls with backoff, got %d", calls)
	}
	if !inFlight {
		t.Errorf("retried update dropped")
	}
}
