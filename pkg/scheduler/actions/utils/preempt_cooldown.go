// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"time"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
)

// Preempt cool-down: once a preemption or reclaim is committed for a job, that job may not trigger another one
// while pods evicted since then are still terminating, for at most the session's PreemptCooldown. A session
// only sees the victims' resources as freed once they are gone (a terminating pod still holds, e.g., its DRA
// claim), so without the cool-down every session can find the same job "placeable" over a fresh set of
// victims and evict them, while the first set is still terminating.

// RecordPreemption marks a committed preemption or reclaim for job at the given time (taken before the
// statement was committed, so every pod it evicted got its deletion timestamp at or after it).
func RecordPreemption(ssn *framework.Session, job *podgroup_info.PodGroupInfo, at time.Time, victims int) {
	cooldown := ssn.PreemptCooldown()
	if cooldown <= 0 || victims == 0 {
		return
	}
	job.LastPreemptionTimestamp = &at
	// TODO(II-2486): remove after rollout verification.
	log.InfraLogger.Infof("[preempt-cooldown] recorded preemption for job <%s>: victims=%d window_end=%s",
		job.NamespacedName, victims, at.Add(cooldown).UTC().Format(time.RFC3339))
}

// preemptCooldownClockSkew is how far the scheduler's clock may be ahead of the API server's, which stamps pod
// deletion timestamps, before a victim of the recorded preemption looks as if it was deleted earlier.
const preemptCooldownClockSkew = 2 * time.Second

// preemptCooldown decides, for one action's input jobs, which jobs are still in their cool-down.
type preemptCooldown struct {
	ssn      *framework.Session
	cooldown time.Duration
	// releasingDeleted holds, for every terminating pod in the session, its deletion timestamp; a pod evicted in
	// this session (not deleted yet) has the zero time. Collected on first use, together with the two
	// summaries below that make blocks constant-time.
	releasingDeleted []time.Time
	// anyNotDeleted: some terminating pod has no deletion timestamp yet.
	anyNotDeleted bool
	// latestDeleted is the latest deletion timestamp among the terminating pods (zero when there is none).
	latestDeleted time.Time
	collected     bool
}

func newPreemptCooldown(ssn *framework.Session) *preemptCooldown {
	return &preemptCooldown{ssn: ssn, cooldown: ssn.PreemptCooldown()}
}

// blocks reports whether job may not trigger evictions now. A job is blocked while it is within
// PreemptCooldown of its last preemption and some pod evicted since then is still terminating. When no such
// pod is left the cool-down ends early and the job's timestamp is cleared.
func (pc *preemptCooldown) blocks(job *podgroup_info.PodGroupInfo, now time.Time) bool {
	end := job.PreemptCooldownEnd(pc.cooldown)
	if end == nil || !now.Before(*end) {
		return false
	}
	since := cooldownSince(*job.LastPreemptionTimestamp)
	pc.collect()
	if !pc.anyNotDeleted && pc.latestDeleted.Before(since) {
		job.LastPreemptionTimestamp = nil
		// TODO(II-2486): remove after rollout verification.
		log.InfraLogger.Infof("[preempt-cooldown] window for job <%s> ended early: its victims are gone",
			job.NamespacedName)
		return false
	}
	if job.MarkPreemptCooldownLogged() {
		// TODO(II-2486): remove after rollout verification.
		log.InfraLogger.Infof(
			"[preempt-cooldown] skipping job <%s> as preemptor: seconds_left=%d releasing_victims=%d",
			job.NamespacedName, int(end.Sub(now).Seconds()), pc.countTerminatingSince(since))
	}
	return true
}

// cooldownSince is the earliest deletion time of a pod evicted by a preemption recorded at last. Annotation
// timestamps have second precision, so it is truncated to the second, after allowing for
// preemptCooldownClockSkew.
func cooldownSince(last time.Time) time.Time {
	return last.Add(-preemptCooldownClockSkew).Truncate(time.Second)
}

// collect gathers the terminating pods of the session once.
func (pc *preemptCooldown) collect() {
	if pc.collected {
		return
	}
	pc.collected = true
	for _, node := range pc.ssn.ClusterInfo.Nodes {
		for _, pod := range node.PodInfos {
			if pod.Status != pod_status.Releasing && pod.Status != pod_status.StuckInReleasing {
				continue
			}
			var deleted time.Time
			if pod.Pod != nil && pod.Pod.DeletionTimestamp != nil {
				deleted = pod.Pod.DeletionTimestamp.Time
			}
			pc.releasingDeleted = append(pc.releasingDeleted, deleted)
			if deleted.IsZero() {
				pc.anyNotDeleted = true
			} else if deleted.After(pc.latestDeleted) {
				pc.latestDeleted = deleted
			}
		}
	}
}

// countTerminatingSince counts terminating pods deleted at or after since (or not deleted yet). It is only
// needed for the log line, so it is not on the path of blocks.
func (pc *preemptCooldown) countTerminatingSince(since time.Time) int {
	count := 0
	for _, deleted := range pc.releasingDeleted {
		if deleted.IsZero() || !deleted.Before(since) {
			count++
		}
	}
	return count
}
