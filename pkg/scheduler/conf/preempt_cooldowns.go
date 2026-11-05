// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"sync"
	"time"
)

// PreemptCooldowns holds, across sessions, when each job last had a preemption or reclaim committed for it.
// It lives in scheduler memory only: a scheduler restart forgets every open cool-down window.
type PreemptCooldowns struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func NewPreemptCooldowns() *PreemptCooldowns {
	return &PreemptCooldowns{last: map[string]time.Time{}}
}

// Record stores at as the job's last committed preemption. A nil store records nothing.
func (pc *PreemptCooldowns) Record(job string, at time.Time) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.last[job] = at
}

// Forget removes the job's window. A nil store holds nothing.
func (pc *PreemptCooldowns) Forget(job string) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	delete(pc.last, job)
}

// Open returns the job's last committed preemption while its window, cooldown long, is still open at now.
// It drops every window that has ended, and every job that keep reports false for, so the store only holds
// jobs that still exist. A nil store, or a cooldown <= 0, has no open windows.
func (pc *PreemptCooldowns) Open(now time.Time, cooldown time.Duration, keep func(job string) bool) map[string]time.Time {
	if pc == nil {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	open := make(map[string]time.Time, len(pc.last))
	for job, at := range pc.last {
		if cooldown <= 0 || !now.Before(at.Add(cooldown)) || !keep(job) {
			delete(pc.last, job)
			continue
		}
		open[job] = at
	}
	return open
}
