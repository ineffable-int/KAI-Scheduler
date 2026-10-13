// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package solvers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
)

type victimGPUsBuilder struct {
	gpus map[common_info.PodID]float64
	next int
}

// victims returns a solution whose victims had the given GPU quotas before the simulation.
func (b *victimGPUsBuilder) victims(gpus ...float64) *solutionResult {
	victims := make([]*pod_info.PodInfo, 0, len(gpus))
	for _, g := range gpus {
		uid := common_info.PodID(fmt.Sprintf("victim-%d", b.next))
		b.next++
		b.gpus[uid] = g
		victims = append(victims, &pod_info.PodInfo{
			UID:                    uid,
			AcceptedGpuRequirement: *resource_info.NewGpuResourceRequirementWithGpus(g, 0),
		})
	}
	return &solutionResult{solved: true, victimsTasks: victims}
}

// The pruned re-simulation is a fresh placement and can evict more than the original
// solution; only a strictly smaller solution may replace it.
func TestPrunedIsSmaller(t *testing.T) {
	b := &victimGPUsBuilder{gpus: map[common_info.PodID]float64{}}
	original := b.victims(4, 4, 4)

	require.True(t, prunedIsSmaller(b.victims(4, 4), original, b.gpus), "fewer pods, fewer GPUs")
	require.False(t, prunedIsSmaller(b.victims(4, 4, 4), original, b.gpus), "same pod count")
	require.False(t, prunedIsSmaller(b.victims(4, 4, 4, 4), original, b.gpus), "more pods")
	require.False(t, prunedIsSmaller(b.victims(8, 8), original, b.gpus), "fewer pods but more GPUs")
	require.True(t, prunedIsSmaller(b.victims(8, 4), original, b.gpus), "fewer pods, equal GPUs")
	require.True(t, prunedIsSmaller(b.victims(0.1, 0.2), b.victims(0.3, 0, 0), b.gpus),
		"equal fractional totals despite float rounding")
}

// The simulation rewrites AcceptedGpuRequirement when it re-places a victim (a GPU-memory
// request becomes a fraction of the destination GPU), so totals use the quotas from before.
func TestPrunedIsSmallerUsesPreSimulationQuotas(t *testing.T) {
	b := &victimGPUsBuilder{gpus: map[common_info.PodID]float64{}}
	original := b.victims(0.5, 0, 0)
	pruned := b.victims(0.5, 0.25)
	pruned.victimsTasks[0].AcceptedGpuRequirement = *resource_info.NewGpuResourceRequirementWithGpus(0.25, 0)

	require.False(t, prunedIsSmaller(pruned, original, b.gpus))
}
