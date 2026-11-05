// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogThrottle(t *testing.T) {
	start := time.Date(2026, 11, 5, 22, 0, 0, 0, time.UTC)
	lt := newLogThrottle(10 * time.Minute)

	assert.True(t, lt.allow("ns/job preempt", "usable=0 excluded=33", start), "first decision")
	assert.False(t, lt.allow("ns/job preempt", "usable=0 excluded=33", start.Add(time.Second)), "same decision next session")
	assert.True(t, lt.allow("ns/job preempt", "usable=18 excluded=7", start.Add(2*time.Second)), "decision changed")
	assert.True(t, lt.allow("ns/other preempt", "usable=18 excluded=7", start.Add(2*time.Second)), "other preemptor")
	assert.False(t, lt.allow("ns/job preempt", "usable=18 excluded=7", start.Add(9*time.Minute)), "unchanged, too soon")
	assert.True(t, lt.allow("ns/job preempt", "usable=18 excluded=7", start.Add(11*time.Minute)), "unchanged, interval passed")
	assert.Len(t, lt.last, 1, "entries older than the interval are dropped")
}
