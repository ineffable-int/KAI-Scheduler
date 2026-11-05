// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"sync"
	"time"
)

// victimFilterLogInterval is how often an unchanged [victim-filter] decision is logged again for one preemptor.
// TODO(II-2486): remove with the log line after rollout verification.
const victimFilterLogInterval = 10 * time.Minute

// victimFilterLogs remembers, across sessions, the last [victim-filter] line logged per preemptor and action. A
// pending job retries every session, so without it one job that can never be placed logs a line per second.
var victimFilterLogs = newLogThrottle(victimFilterLogInterval)

type logThrottle struct {
	interval time.Duration
	mu       sync.Mutex
	last     map[string]loggedDecision
}

type loggedDecision struct {
	decision string
	at       time.Time
}

func newLogThrottle(interval time.Duration) *logThrottle {
	return &logThrottle{interval: interval, last: map[string]loggedDecision{}}
}

// allow reports whether decision should be logged for key at now: the first time, when it differs from the last
// one logged, or when the last one is at least interval old. Entries older than interval are dropped, so the map
// only holds preemptors seen recently.
func (lt *logThrottle) allow(key, decision string, now time.Time) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	for k, logged := range lt.last {
		if now.Sub(logged.at) >= lt.interval {
			delete(lt.last, k)
		}
	}
	if logged, found := lt.last[key]; found && logged.decision == decision {
		return false
	}
	lt.last[key] = loggedDecision{decision: decision, at: now}
	return true
}
