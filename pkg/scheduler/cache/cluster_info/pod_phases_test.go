// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package cluster_info

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
)

// PodPhases covers every listed pod, also those on no node and in no PodGroup, which the
// snapshot keeps nowhere else.
func TestSnapshotPodPhasesCoverUnboundPods(t *testing.T) {
	pod := func(name, node string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test", UID: k8stypes.UID("uid-" + name)},
			Spec:       corev1.PodSpec{NodeName: node},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	clusterInfo := newClusterInfoTestsInner(t,
		[]runtime.Object{pod("unbound", "", corev1.PodPending), pod("done", "", corev1.PodSucceeded)},
		[]runtime.Object{},
		&conf.SchedulingNodePoolParams{NodePoolLabelKey: nodePoolNameLabel},
		true, nil, nil)

	snapshot, err := clusterInfo.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, corev1.PodPending, snapshot.PodPhases["uid-unbound"])
	assert.Equal(t, corev1.PodSucceeded, snapshot.PodPhases["uid-done"])
	assert.Len(t, snapshot.PodPhases, 2)
}
