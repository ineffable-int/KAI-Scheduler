// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package solvers

import (
	"golang.org/x/exp/maps"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common/solvers/scenario"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
)

type SolutionValidator func(scenario api.ScenarioInfo) bool

type simulationVictims struct {
	preemptedVictims []*pod_info.PodInfo
	pipelinedVictims []*pod_info.PodInfo
}

func newCalculatedVictimsStruct() *simulationVictims {
	return &simulationVictims{
		preemptedVictims: make([]*pod_info.PodInfo, 0),
		pipelinedVictims: make([]*pod_info.PodInfo, 0),
	}
}

type solutionResult struct {
	solved       bool
	victimsTasks []*pod_info.PodInfo
	victimJobs   []*podgroup_info.PodGroupInfo
	statement    *framework.Statement
}

type byPodSolver struct {
	feasibleNodes            map[string]*node_info.NodeInfo
	solutionValidator        SolutionValidator
	allowVictimConsolidation bool
	pruneVictims             bool
	actionType               framework.ActionType
}

func newByPodSolver(
	feasibleNodes map[string]*node_info.NodeInfo,
	checkVictims SolutionValidator,
	allowVictimConsolidation bool,
	pruneVictims bool,
	action framework.ActionType,
) *byPodSolver {
	return &byPodSolver{
		feasibleNodes:            feasibleNodes,
		solutionValidator:        checkVictims,
		allowVictimConsolidation: allowVictimConsolidation,
		pruneVictims:             pruneVictims,
		actionType:               action,
	}
}

// solve evaluates a scenario's feasibility by simulating it: evicting its victims and simulating the allocation loop.
// This simulates allocation order, node sorting, predicates, and all relevant scheduling logic, so the scheduler will
// not perform evictions that will not result in successful allocation of the pending job. This also helps the scheduler
// avoid evictions which are not relevant to the specific scenario.
func (s *byPodSolver) solve(session *framework.Session, scenario *scenario.ByNodeScenario) *solutionResult {
	statement := session.Statement()

	pendingJob := scenario.GetPreemptor()
	nextTaskToFindAllocation := scenario.PendingTasks()[len(scenario.PendingTasks())-1]

	allVictims := getVictimTasks(scenario.RecordedVictimsTasks(), scenario.PotentialVictimsTasks())

	if len(allVictims) == 0 {
		statement.Discard()
		return &solutionResult{false, nil, nil, nil}
	}

	originalNodes := make(map[common_info.PodID]string, len(allVictims))
	originalGPUs := make(map[common_info.PodID]float64, len(allVictims))
	for _, victim := range allVictims {
		originalNodes[victim.UID] = victim.NodeName
		originalGPUs[victim.UID] = victim.AcceptedGpuRequirement.GetGpusQuota()
	}

	checkpoint := statement.Checkpoint()
	if err := common.EvictAllPreemptees(session, allVictims, pendingJob, statement, s.actionType); err != nil {
		return handleSolveError(pendingJob, nextTaskToFindAllocation, err, statement)
	}
	newFeasibleNodes := s.updateFeasibleNodes(session, allVictims)

	result := s.runSimulation(session, scenario, statement, allVictims, maps.Values(s.feasibleNodes))
	if result != nil {
		if result.solved && s.pruneVictims {
			if pruned := s.pruneSolution(session, scenario, statement, allVictims, originalNodes, originalGPUs, result); pruned != nil {
				result = pruned
			}
		}
		// Roll back on validator-rejected and error results too: the map is
		// shared across the probe's scenarios and must stay derived from the
		// solver's node set plus the recorded victims.
		if !result.solved {
			s.feasibleNodesRollback(newFeasibleNodes)
		}
		return result
	}

	s.feasibleNodesRollback(newFeasibleNodes)
	if err := statement.Rollback(checkpoint); err != nil {
		return handleSolveError(pendingJob, nextTaskToFindAllocation, err, statement)
	}
	statement.Discard()
	return &solutionResult{false, nil, nil, nil}
}

func (s *byPodSolver) runSimulation(
	session *framework.Session, scenario *scenario.ByNodeScenario, statement *framework.Statement,
	victimTasks []*pod_info.PodInfo, nodes []*node_info.NodeInfo) *solutionResult {
	successfulSimulation, solutionVictims :=
		s.tryScenarioWithEvictedVictims(session, scenario, statement, victimTasks, nodes)
	if successfulSimulation {
		return s.handleScenarioSolution(scenario, statement, solutionVictims)
	}
	return nil
}

// pruneSolution keeps only the victim batches (a whole gang, or one pod of an elastic job)
// with a pod on a node where the simulation placed a preemptor task. The other batches free
// nothing the preemptor uses, yet they are still deleted on commit: the simulation either
// re-placed them on another node (Pipelined, which does not move the pod) or could not
// re-place them. The kept batches are re-simulated as a fresh placement, which can land
// differently, so the pruned solution is kept only if it is smaller than the original
// (see prunedIsSmaller); otherwise the original solution is rebuilt. Returns nil when
// there is nothing to prune, or when the original solution evicts no pod of a dropped batch.
func (s *byPodSolver) pruneSolution(
	session *framework.Session, sc *scenario.ByNodeScenario, statement *framework.Statement,
	allVictims []*pod_info.PodInfo, originalNodes map[common_info.PodID]string,
	originalGPUs map[common_info.PodID]float64, original *solutionResult,
) *solutionResult {
	preemptorNodes := map[string]bool{}
	preemptorUID := sc.GetPreemptor().UID
	for nodeName, node := range s.feasibleNodes {
		if node == nil {
			continue
		}
		for _, task := range node.PodInfos {
			if task.Job == preemptorUID &&
				(task.Status == pod_status.Pipelined || task.Status == pod_status.Allocated) {
				preemptorNodes[nodeName] = true
				break
			}
		}
	}
	if len(preemptorNodes) == 0 {
		return nil
	}

	type batch struct{ tasks []*pod_info.PodInfo }
	batches := map[*podgroup_info.PodGroupInfo]*batch{}
	var batchOrder []*podgroup_info.PodGroupInfo
	keep := map[*podgroup_info.PodGroupInfo]bool{}
	for _, victim := range allVictims {
		rep := sc.GetVictimJobRepresentativeById(victim)
		if rep == nil {
			return nil
		}
		if _, found := batches[rep]; !found {
			batches[rep] = &batch{}
			batchOrder = append(batchOrder, rep)
		}
		batches[rep].tasks = append(batches[rep].tasks, victim)
		if preemptorNodes[originalNodes[victim.UID]] {
			keep[rep] = true
		}
	}
	evicted := make(map[common_info.PodID]bool, len(original.victimsTasks))
	for _, victim := range original.victimsTasks {
		evicted[victim.UID] = true
	}
	var keptBatches [][]*pod_info.PodInfo
	var keptTasks []*pod_info.PodInfo
	droppedEvicted := false
	for _, rep := range batchOrder {
		if keep[rep] {
			keptBatches = append(keptBatches, batches[rep].tasks)
			keptTasks = append(keptTasks, batches[rep].tasks...)
			continue
		}
		for _, task := range batches[rep].tasks {
			droppedEvicted = droppedEvicted || evicted[task.UID]
		}
	}
	// The original solution already leaves every dropped batch in place. Pruning exists to
	// spare evicted pods of dropped batches, so it would cost two simulations for, at best,
	// a different placement of the kept ones.
	if len(keptTasks) == 0 || !droppedEvicted {
		return nil
	}

	pendingJob := sc.GetPreemptor()
	nextTask := sc.PendingTasks()[len(sc.PendingTasks())-1]
	nodes := maps.Values(s.feasibleNodes)

	statement.Discard()
	// AddPotentialVictimsTasks clones the tasks, so build the scenario after the discard
	// for the clones to carry their pre-simulation node and status.
	pruned := scenario.NewByNodeScenario(session, sc.GetPreemptor(), sc.PendingTasks(), nil, nil)
	for _, tasks := range keptBatches {
		pruned.AddPotentialVictimsTasks(tasks)
	}
	if err := common.EvictAllPreemptees(session, keptTasks, pendingJob, statement, s.actionType); err == nil {
		if result := s.runSimulation(session, pruned, statement, keptTasks, nodes); result != nil && result.solved {
			if prunedIsSmaller(result, original, originalGPUs) {
				log.InfraLogger.V(4).Infof("Pruned victims for <%s/%s>: %d -> %d",
					pendingJob.Namespace, pendingJob.Name, len(original.victimsTasks), len(result.victimsTasks))
				return result
			}
			log.InfraLogger.V(4).Infof("Pruned solution for <%s/%s> is not smaller (%d -> %d victims), keeping the original",
				pendingJob.Namespace, pendingJob.Name, len(original.victimsTasks), len(result.victimsTasks))
		}
	}

	statement.Discard()
	if err := common.EvictAllPreemptees(session, allVictims, pendingJob, statement, s.actionType); err != nil {
		return handleSolveError(pendingJob, nextTask, err, statement)
	}
	if result := s.runSimulation(session, sc, statement, allVictims, nodes); result != nil {
		return result
	}
	statement.Discard()
	return &solutionResult{false, nil, nil, nil}
}

// prunedIsSmaller accepts a pruned solution only if it evicts fewer pods and no more GPUs
// than the original. GPU totals use the quotas from before the simulations (originalGPUs),
// because a simulation rewrites a re-placed victim's AcceptedGpuRequirement.
func prunedIsSmaller(pruned, original *solutionResult, originalGPUs map[common_info.PodID]float64) bool {
	return len(pruned.victimsTasks) < len(original.victimsTasks) &&
		resource_info.LessOrEqualWithTolerance(
			victimGPUs(pruned.victimsTasks, originalGPUs), victimGPUs(original.victimsTasks, originalGPUs))
}

func victimGPUs(victims []*pod_info.PodInfo, originalGPUs map[common_info.PodID]float64) float64 {
	total := 0.0
	for _, victim := range victims {
		total += originalGPUs[victim.UID]
	}
	return total
}

func (s *byPodSolver) feasibleNodesRollback(newFeasibleNodes map[string]bool) {
	for potentialNodeFeasibleNode := range newFeasibleNodes {
		delete(s.feasibleNodes, potentialNodeFeasibleNode)
	}
}

func (s *byPodSolver) updateFeasibleNodes(ssn *framework.Session, victimTasks []*pod_info.PodInfo) map[string]bool {
	newFeasibleNodes := map[string]bool{}
	for _, potentialVictimTasks := range victimTasks {
		_, found := s.feasibleNodes[potentialVictimTasks.NodeName]
		if !found {
			newFeasibleNodes[potentialVictimTasks.NodeName] = true
		}
		s.feasibleNodes[potentialVictimTasks.NodeName] = ssn.ClusterInfo.Nodes[potentialVictimTasks.NodeName]
	}
	return newFeasibleNodes
}

func (s *byPodSolver) handleScenarioSolution(
	scenario *scenario.ByNodeScenario, statement *framework.Statement, solutionVictims *simulationVictims,
) *solutionResult {
	victimsTasks := make([]*pod_info.PodInfo, len(solutionVictims.preemptedVictims))
	copy(victimsTasks, solutionVictims.preemptedVictims)
	if !s.allowVictimConsolidation {
		victimsTasks = append(victimsTasks, solutionVictims.pipelinedVictims...)
	}
	actualVictimJobs := getVictimJobsFromVictimTasks(victimsTasks, scenario)

	if s.solutionValidator != nil {
		validSolution := s.solutionValidator(scenario)
		if !validSolution {
			statement.Discard()
			return &solutionResult{false, nil, nil, nil}
		}
	}

	if s.allowVictimConsolidation {
		victimsTasks = append(victimsTasks, solutionVictims.pipelinedVictims...)
		actualVictimJobs = getVictimJobsFromVictimTasks(victimsTasks, scenario)
	}

	return &solutionResult{true, victimsTasks, actualVictimJobs, statement}
}

func (s *byPodSolver) tryScenarioWithEvictedVictims(ssn *framework.Session, scenario *scenario.ByNodeScenario,
	statement *framework.Statement, victimTasks []*pod_info.PodInfo, nodes []*node_info.NodeInfo) (bool, *simulationVictims) {
	pendingJob := scenario.GetPreemptor()

	jobsToAllocate := common.GetJobsToAllocate(ssn, victimTasks, pendingJob)
	isSuccessfulAllocations, _ :=
		common.TryToVirtuallyAllocatePreemptorAndGetVictims(ssn, statement, nodes, pendingJob,
			jobsToAllocate, victimTasks, podgroup_info.PartialTaskAllocation)

	if !isSuccessfulAllocations {
		return false, nil
	}
	actualVictims := newCalculatedVictimsStruct()
	for _, victimTask := range victimTasks {
		switch victimTask.Status {
		case pod_status.Releasing:
			actualVictims.preemptedVictims = append(actualVictims.preemptedVictims, victimTask)
		case pod_status.Pipelined:
			actualVictims.pipelinedVictims = append(actualVictims.pipelinedVictims, victimTask)
		}
	}
	return isSuccessfulAllocations, actualVictims
}

func getVictimJobsFromVictimTasks(
	actualVictimsTasks []*pod_info.PodInfo, scenario *scenario.ByNodeScenario) []*podgroup_info.PodGroupInfo {
	actualVictimJobs := extractJobsFromTasks(actualVictimsTasks, scenario)

	victimJobsAsList := make([]*podgroup_info.PodGroupInfo, 0)
	for _, victimJobsSameJobBase := range actualVictimJobs {
		victimJobsAsList = append(victimJobsAsList, victimJobsSameJobBase...)
	}
	return victimJobsAsList
}

func extractJobsFromTasks(
	tasks []*pod_info.PodInfo, scenario *scenario.ByNodeScenario) map[common_info.PodGroupID][]*podgroup_info.PodGroupInfo {
	jobs := map[common_info.PodGroupID][]*podgroup_info.PodGroupInfo{}
	for _, task := range tasks {
		jobAlreadyExists := false
		if possibleDuplicates, ok := jobs[task.Job]; ok {
			for _, possibleDuplicate := range possibleDuplicates {
				for _, podInfo := range possibleDuplicate.GetAllPodsMap() {
					if podInfo.UID == task.UID {
						jobAlreadyExists = true
						break
					}
				}
				if jobAlreadyExists {
					break
				}
			}
		}
		if !jobAlreadyExists {
			matchingJob := scenario.GetVictimJobRepresentativeById(task)
			jobs[matchingJob.UID] = append(jobs[matchingJob.UID], matchingJob)
		}
	}
	return jobs
}

func getVictimTasks(recordedVictimsTasks []*pod_info.PodInfo, potentialVictimsTasks []*pod_info.PodInfo) []*pod_info.PodInfo {
	victimTasks := make([]*pod_info.PodInfo, len(recordedVictimsTasks)+len(potentialVictimsTasks))
	copy(victimTasks, recordedVictimsTasks)
	copy(victimTasks[len(recordedVictimsTasks):], potentialVictimsTasks)
	return victimTasks
}

func handleSolveError(pendingJob *podgroup_info.PodGroupInfo, nextTaskToFindAllocation *pod_info.PodInfo, err error,
	statement *framework.Statement,
) *solutionResult {
	log.InfraLogger.V(6).Do(func() {
		log.InfraLogger.Infof("Could not attempt to allocate over victims for pending job <%s/%s> <%v> "+
			"while simulation pod allocation %v due to error: %v",
			pendingJob.Namespace, pendingJob.Name, pendingJob.GetAliveTasksRequestedGPUs(), nextTaskToFindAllocation,
			err)
	})
	statement.Discard()
	return &solutionResult{false, nil, nil, nil}
}

func nodeIdleOrReleasingGpus(ni *node_info.NodeInfo) float64 {
	idle, _ := ni.GetSumOfIdleGPUs()
	releasing, _ := ni.GetSumOfReleasingGPUs()
	return idle + releasing
}
