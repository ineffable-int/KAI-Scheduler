// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"testing"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
)

// Five non-victim pods of 0.2 GPU on an 8 GPU node leave exactly 7 GPUs, but the float subtraction
// gives 6.999999999999999. The node must still count 7 pods, or the filter would drop its rack.
func TestPodsThatFitAbsorbsFloatError(t *testing.T) {
	vectorMap := resource_info.NewResourceVectorMap()
	node := &node_info.NodeInfo{
		AllocatableVector: resource_info.NewResourceVector(vectorMap),
		VectorMap:         vectorMap,
		PodInfos:          map[common_info.PodID]*pod_info.PodInfo{},
	}
	node.AllocatableVector.Set(resource_info.GPUIndex, 8)
	for i := 0; i < 5; i++ {
		id := common_info.PodID(string(rune('a' + i)))
		node.PodInfos[id] = &pod_info.PodInfo{
			Job:    "non-victim",
			Status: pod_status.Running,
			GpuRequirement: *resource_info.NewGpuResourceRequirementWithMultiFraction(
				1, 0.2, 0),
		}
	}
	victims := map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{}

	if free := freeableGPUs(node, victims); free >= 7 {
		t.Fatalf("freeableGPUs = %v, expected float error below 7 for this test to be meaningful", free)
	}
	if got := podsThatFit(node, victims, 1); got != 7 {
		t.Errorf("podsThatFit = %d, want 7", got)
	}
}
