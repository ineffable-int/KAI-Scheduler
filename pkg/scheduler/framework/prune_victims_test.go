// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package framework

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
)

// Snapshots store SchedulerParams as JSON. One taken before the setting existed has no key
// for it and must replay with pruning on, the binary's default.
func TestPruneVictimsFromSnapshotParams(t *testing.T) {
	for raw, want := range map[string]bool{
		`{}`:                             true,
		`{"disableVictimPruning":false}`: true,
		`{"disableVictimPruning":true}`:  false,
	} {
		var params conf.SchedulerParams
		require.NoError(t, json.Unmarshal([]byte(raw), &params))
		ssn := &Session{SchedulerParams: params}
		require.Equal(t, want, ssn.PruneVictims(), raw)
	}
}
