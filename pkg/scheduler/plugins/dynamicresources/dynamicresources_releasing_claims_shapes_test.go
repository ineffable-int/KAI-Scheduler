// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package dynamicresources_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/dynamicresources"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
)

// openPipelineOverReleasing opens a fresh dynamicresources plugin with the argument on over
// ssn's cluster, after a test has changed ClusterInfo.
func openPipelineOverReleasing(ssn *framework.Session) *framework.Session {
	draSession := &framework.Session{ClusterInfo: ssn.ClusterInfo, Cache: ssn.Cache, SchedulerParams: ssn.SchedulerParams}
	dynamicresources.New(framework.PluginArguments{dynamicresources.PipelineOverReleasingClaimsKey: "true"}).
		OnSessionOpen(draSession)
	return draSession
}

// A claim reserved for a live pod the session does not track (another scheduler's pod, not
// bound to a node yet) is held, not released: only a pod missing from the cluster is deleted.
func TestPipelineOverReleasingClaims_LiveUntrackedConsumerHolds(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	topology := releasingClaimTopology(t, pod_status.Releasing, true, 0)
	topology.Jobs = topology.Jobs[1:] // the holder is not a session pod
	ssn := setupTestSession(t, topology, gomock.NewController(t))
	ssn.ClusterInfo.PodPhases["holder-0"] = v1.PodPending

	draSession := openPipelineOverReleasing(ssn)
	task := getTaskByStatus(t, draSession, "pending", pod_status.Pending)
	job := draSession.ClusterInfo.PodGroupInfos[task.Job]
	require.NoError(t, draSession.PrePredicateFn(task, job))
	require.Error(t, draSession.PredicateFn(task, job, draSession.ClusterInfo.Nodes["node0"]))
}
