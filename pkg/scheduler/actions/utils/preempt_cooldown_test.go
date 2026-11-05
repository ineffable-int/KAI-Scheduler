// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/queue_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
)

// recordingLogger keeps the Infof lines so the temporary [preempt-cooldown] log lines can be checked.
type recordingLogger struct {
	log.SchedulerLogger
	lines []string
}

func (l *recordingLogger) Infof(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) cooldownLines() []string {
	var lines []string
	for _, line := range l.lines {
		if strings.HasPrefix(line, "[preempt-cooldown]") {
			lines = append(lines, line)
		}
	}
	return lines
}

func recordLogs(t *testing.T) *recordingLogger {
	logger := &recordingLogger{SchedulerLogger: log.InfraLogger}
	previous := log.InfraLogger
	log.InfraLogger = logger
	t.Cleanup(func() { log.InfraLogger = previous })
	return logger
}

type cooldownPod struct {
	status  pod_status.PodStatus
	deleted *time.Time // deletion timestamp; nil for a pod evicted in this session (not deleted yet)
}

func cooldownSession(cooldown time.Duration, pods ...cooldownPod) *framework.Session {
	podInfos := map[common_info.PodID]*pod_info.PodInfo{}
	for i, p := range pods {
		pod := &v1.Pod{}
		if p.deleted != nil {
			pod.DeletionTimestamp = &metav1.Time{Time: *p.deleted}
		}
		podInfos[common_info.PodID(fmt.Sprintf("pod-%d", i))] = &pod_info.PodInfo{Status: p.status, Pod: pod}
	}
	return &framework.Session{
		ClusterInfo: &api.ClusterInfo{
			Nodes: map[string]*node_info.NodeInfo{"node-0": {Name: "node-0", PodInfos: podInfos}},
		},
		SchedulerParams: conf.SchedulerParams{PreemptCooldown: cooldown},
	}
}

func preemptorJob() *podgroup_info.PodGroupInfo {
	job := podgroup_info.NewPodGroupInfo("ns/gang")
	job.NamespacedName = "ns/gang"
	return job
}

func TestRecordPreemption(t *testing.T) {
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name     string
		cooldown time.Duration
		victims  int
		want     *time.Time
		wantLog  []string
	}{
		{
			name: "records the time and logs the window", cooldown: 3 * time.Minute, victims: 4, want: &at,
			wantLog: []string{"[preempt-cooldown] recorded preemption for job <ns/gang>: victims=4 " +
				"window_end=2026-11-05T12:03:00Z"},
		},
		{name: "disabled", cooldown: 0, victims: 4},
		{name: "no victims", cooldown: 3 * time.Minute, victims: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logs := recordLogs(t)
			job := preemptorJob()
			RecordPreemption(cooldownSession(tt.cooldown), job, at, tt.victims)
			assert.Equal(t, tt.want, job.LastPreemptionTimestamp)
			assert.Equal(t, tt.wantLog, logs.cooldownLines())
		})
	}
}

func TestPreemptCooldownBlocks(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 1, 0, 0, time.UTC)
	preempted := now.Add(-time.Minute).Add(300 * time.Millisecond) // annotation precision drops the fraction
	before := preempted.Add(-10 * time.Second)
	sameSecond := preempted.Truncate(time.Second)
	after := preempted.Add(5 * time.Second)
	skewed := preempted.Add(-1500 * time.Millisecond) // scheduler clock ahead of the API server's

	for _, tt := range []struct {
		name          string
		cooldown      time.Duration
		lastPreempted *time.Time
		pods          []cooldownPod
		wantBlocked   bool
		wantCleared   bool
		wantLog       []string
	}{
		{
			name:     "never preempted",
			cooldown: 3 * time.Minute,
			pods:     []cooldownPod{{status: pod_status.Releasing, deleted: &after}},
		},
		{
			name:          "disabled",
			cooldown:      0,
			lastPreempted: &preempted,
			pods:          []cooldownPod{{status: pod_status.Releasing, deleted: &after}},
		},
		{
			name:          "victims still terminating: blocked, counts only pods deleted since the preemption",
			cooldown:      3 * time.Minute,
			lastPreempted: &preempted,
			pods: []cooldownPod{
				{status: pod_status.Releasing, deleted: &after},
				{status: pod_status.StuckInReleasing, deleted: &sameSecond},
				{status: pod_status.Releasing}, // evicted earlier in this session
				{status: pod_status.Releasing, deleted: &before},
				{status: pod_status.Running},
			},
			wantBlocked: true,
			wantLog: []string{
				"[preempt-cooldown] skipping job <ns/gang> as preemptor: seconds_left=120 releasing_victims=3",
			},
		},
		{
			name:          "victim deleted slightly before the recorded time (clock skew) still blocks",
			cooldown:      3 * time.Minute,
			lastPreempted: &preempted,
			pods:          []cooldownPod{{status: pod_status.Releasing, deleted: &skewed}},
			wantBlocked:   true,
			wantLog: []string{
				"[preempt-cooldown] skipping job <ns/gang> as preemptor: seconds_left=120 releasing_victims=1",
			},
		},
		{
			name:          "victims gone: window ends early and the timestamp is cleared",
			cooldown:      3 * time.Minute,
			lastPreempted: &preempted,
			pods: []cooldownPod{
				{status: pod_status.Releasing, deleted: &before},
				{status: pod_status.Running},
			},
			wantCleared: true,
			wantLog:     []string{"[preempt-cooldown] window for job <ns/gang> ended early: its victims are gone"},
		},
		{
			name:          "window elapsed",
			cooldown:      30 * time.Second,
			lastPreempted: &preempted,
			pods:          []cooldownPod{{status: pod_status.Releasing, deleted: &after}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logs := recordLogs(t)
			job := preemptorJob()
			job.LastPreemptionTimestamp = tt.lastPreempted
			cooldown := newPreemptCooldown(cooldownSession(tt.cooldown, tt.pods...))

			assert.Equal(t, tt.wantBlocked, cooldown.blocks(job, now))
			// A second action in the same session decides the same way but does not log the skip again.
			assert.Equal(t, tt.wantBlocked, cooldown.blocks(job, now))

			if tt.wantCleared {
				assert.Nil(t, job.LastPreemptionTimestamp)
			} else {
				assert.Equal(t, tt.lastPreempted, job.LastPreemptionTimestamp)
			}
			assert.Equal(t, tt.wantLog, logs.cooldownLines())
		})
	}
}

// TestInitializeWithJobsPreemptCooldown checks that the cool-down only filters jobs for eviction-triggering
// actions: allocate (no FilterWithinPreemptionDelay) still gets the job.
func TestInitializeWithJobsPreemptCooldown(t *testing.T) {
	recordLogs(t)
	now := time.Now()
	preempted := now.Add(-time.Minute)
	deleted := now.Add(-30 * time.Second)

	for _, tt := range []struct {
		name            string
		evictionTrigger bool
		want            int
	}{
		{name: "preempt / reclaim skip the job", evictionTrigger: true, want: 0},
		{name: "allocate keeps the job", evictionTrigger: false, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ssn := newPrioritySession(t)
			ssn.ClusterInfo = cooldownSession(0, cooldownPod{status: pod_status.Releasing, deleted: &deleted}).ClusterInfo
			ssn.SchedulerParams.PreemptCooldown = 3 * time.Minute
			job := podGroupForJobOrderTest("gang", "gang", 1)
			job.Queue = testQueue
			job.LastPreemptionTimestamp = &preempted
			ssn.ClusterInfo.Queues = map[common_info.QueueID]*queue_info.QueueInfo{testQueue: {UID: testQueue}}
			ssn.ClusterInfo.PodGroupInfos = map[common_info.PodGroupID]*podgroup_info.PodGroupInfo{job.UID: job}

			jobsOrder := NewJobsOrderByQueues(ssn, JobsOrderInitOptions{
				FilterWithinPreemptionDelay: tt.evictionTrigger,
				MaxJobsQueueDepth:           10,
			})
			jobsOrder.InitializeWithJobs(ssn.ClusterInfo.PodGroupInfos)
			require.Equal(t, tt.want, jobsOrder.Len())
		})
	}
}
