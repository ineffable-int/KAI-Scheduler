// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package solvers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
)

func victimsWithGPUs(gpus ...float64) *solutionResult {
	victims := make([]*pod_info.PodInfo, 0, len(gpus))
	for _, g := range gpus {
		victims = append(victims, &pod_info.PodInfo{
			AcceptedGpuRequirement: *resource_info.NewGpuResourceRequirementWithGpus(g, 0),
		})
	}
	return &solutionResult{solved: true, victimsTasks: victims}
}

// The pruned re-simulation is a fresh placement and can evict more than the original
// solution; only a strictly smaller solution may replace it.
func TestPrunedIsSmaller(t *testing.T) {
	original := victimsWithGPUs(4, 4, 4)

	require.True(t, prunedIsSmaller(victimsWithGPUs(4, 4), original), "fewer pods, fewer GPUs")
	require.False(t, prunedIsSmaller(victimsWithGPUs(4, 4, 4), original), "same pod count")
	require.False(t, prunedIsSmaller(victimsWithGPUs(4, 4, 4, 4), original), "more pods")
	require.False(t, prunedIsSmaller(victimsWithGPUs(8, 8), original), "fewer pods but more GPUs")
	require.True(t, prunedIsSmaller(victimsWithGPUs(8, 4), original), "fewer pods, equal GPUs")
}
