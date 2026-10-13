// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package nodelocalgreedy

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common/solvers/scenario"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
)

// elasticVictimScenario builds a scenario whose potential victims are a two-pod gang on the
// first two nodes, then every pod of an elastic job with podsPerNode pods on each node, one
// batch per pod. The first elastic pod is also a recorded victim.
func elasticVictimScenario(t testing.TB, nodeCount, podsPerNode int) (
	*framework.Session, *scenario.ByNodeScenario, []*pod_info.PodInfo, []*pod_info.PodInfo,
) {
	nodeGPUs := map[string]int{}
	var nodeNames []string
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("node-%03d", i)
		nodeGPUs[name] = podsPerNode + 1
		nodeNames = append(nodeNames, name)
	}
	ssn := newGeneratorTestSession(t, nodeGPUs)
	_, gangTasks := addGeneratorTestJob(t, ssn, 2, 1, "team-gang", nodeNames[0], nodeNames[1])
	elastic, elasticTasks := addGeneratorTestJob(t, ssn, nodeCount*podsPerNode, 2, "team-elastic", nodeNames...)
	pending := addGeneratorTestPendingJob(t, ssn, 1, 3, "team-pending")

	recorded := elastic.CloneWithTasks(elasticTasks[:1])
	base := scenario.NewByNodeScenario(ssn, pending, slices.Collect(maps.Values(pending.GetAllPodsMap())), nil,
		[]*podgroup_info.PodGroupInfo{recorded})
	base.AddPotentialVictimsTasks(gangTasks)
	for _, task := range elasticTasks {
		base.AddPotentialVictimsTasks([]*pod_info.PodInfo{task})
	}
	return ssn, base, gangTasks, elasticTasks
}

// Each node's scenario holds only the batches with a pod on that node, whole, minus recorded
// victims: the gang on the nodes it runs on, and only that node's pods of the elastic job.
func TestNodeLocalScenariosTakeOnlyBatchesOnTheNode(t *testing.T) {
	ssn, base, gangTasks, elasticTasks := elasticVictimScenario(t, 3, 2)

	want := map[string][]*pod_info.PodInfo{}
	for _, task := range elasticTasks[1:] {
		want[task.NodeName] = append(want[task.NodeName], task)
	}
	for _, task := range gangTasks {
		want[task.NodeName] = append(want[task.NodeName], gangTasks...)
	}

	scenarios := nodeLocalScenarios(ssn, base)
	require.Len(t, scenarios, len(want))
	got := map[string][]string{}
	for _, sn := range scenarios {
		nodes := map[string]bool{}
		for _, task := range sn.PotentialVictimsTasks() {
			if !isGangTask(task, gangTasks) {
				nodes[task.NodeName] = true
			}
		}
		require.Len(t, nodes, 1, "elastic victims of one scenario must share a node")
		for node := range nodes {
			got[node] = podNames(sn.PotentialVictimsTasks())
		}
	}
	for node, tasks := range want {
		require.Equal(t, podNames(tasks), got[node], node)
	}
}

func isGangTask(task *pod_info.PodInfo, gang []*pod_info.PodInfo) bool {
	for _, g := range gang {
		if g.UID == task.UID {
			return true
		}
	}
	return false
}

func BenchmarkNodeLocalScenariosElastic(b *testing.B) {
	ssn, base, _, _ := elasticVictimScenario(b, 64, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeLocalScenarios(ssn, base)
	}
}
