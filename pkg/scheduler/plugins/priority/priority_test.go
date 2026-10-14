// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package priority

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
)

func TestConsolidationVictimFilterFn(t *testing.T) {
	preemptor := &podgroup_info.PodGroupInfo{Priority: 25}
	assert.False(t, ConsolidationVictimFilterFn(preemptor, &podgroup_info.PodGroupInfo{Priority: 50}))
	assert.False(t, ConsolidationVictimFilterFn(preemptor, &podgroup_info.PodGroupInfo{Priority: 25}))
	assert.True(t, ConsolidationVictimFilterFn(preemptor, &podgroup_info.PodGroupInfo{Priority: 10}))
}

func TestConsolidationRespectPriorityArgument(t *testing.T) {
	for _, tc := range []struct {
		args         framework.PluginArguments
		wantFilterFn bool
	}{
		{args: framework.PluginArguments{}, wantFilterFn: false},
		{args: framework.PluginArguments{ConsolidationRespectPriorityKey: "false"}, wantFilterFn: false},
		{args: framework.PluginArguments{ConsolidationRespectPriorityKey: "true"}, wantFilterFn: true},
		{args: framework.PluginArguments{ConsolidationRespectPriorityKey: "yes please"}, wantFilterFn: false},
	} {
		ssn := &framework.Session{}
		New(tc.args).OnSessionOpen(ssn)
		assert.Equal(t, tc.wantFilterFn, len(ssn.ConsolidationVictimFilterFns) == 1, tc.args)
	}
}
