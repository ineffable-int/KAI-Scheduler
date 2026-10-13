// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package dynamicresources

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
)

func claimReservedFor(uid types.UID) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{Status: resourceapi.ResourceClaimStatus{
		ReservedFor: []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", UID: uid}},
	}}
}

// A pod without a PodGroup is only known from the node it runs on. It must still be found,
// or its claim would look reserved for a deleted pod and count as releasing.
func TestReleasingConsumersSeePodsWithoutPodGroup(t *testing.T) {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "test", UID: "other-0"},
		Spec: v1.PodSpec{NodeName: "node0"}, Status: v1.PodStatus{Phase: v1.PodRunning}}
	running := pod_info.NewTaskInfo(pod, resource_info.NewResourceVectorMap())
	nodes := map[string]*node_info.NodeInfo{
		"node0": {Name: "node0", PodInfos: map[common_info.PodID]*pod_info.PodInfo{pod_info.PodKey(pod): running}},
	}
	drap := &draPlugin{podsByUID: indexSessionPods(map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{}, nodes)}

	assert.Equal(t, pod_status.Running, running.Status)
	assert.False(t, drap.reservedOnlyForReleasingPods(claimReservedFor("other-0")), "running pod without a PodGroup")
	assert.True(t, drap.reservedOnlyForReleasingPods(claimReservedFor("deleted-0")), "deleted pod")
	assert.False(t, drap.reservedOnlyForReleasingPods(&resourceapi.ResourceClaim{}), "no consumers")
}
