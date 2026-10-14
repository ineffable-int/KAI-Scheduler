/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package priority

import (
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
)

const (
	// ConsolidationRespectPriorityKey makes consolidation evict only jobs of strictly lower
	// priority than the job it makes room for, as preempt does.
	ConsolidationRespectPriorityKey     = "consolidationRespectPriority"
	defaultConsolidationRespectPriority = false
)

type priorityPlugin struct {
	consolidationRespectPriority bool
}

func New(pluginArgs framework.PluginArguments) framework.Plugin {
	respectPriority, err := pluginArgs.GetBool(ConsolidationRespectPriorityKey, defaultConsolidationRespectPriority)
	if err != nil {
		log.InfraLogger.Warningf("Failed to parse %s as bool: %v. Using default value of: %t",
			ConsolidationRespectPriorityKey, err, defaultConsolidationRespectPriority)
	}
	return &priorityPlugin{consolidationRespectPriority: respectPriority}
}

func (pp *priorityPlugin) Name() string {
	return "priority"
}

func (pp *priorityPlugin) OnSessionOpen(ssn *framework.Session) {
	ssn.AddJobOrderFn(JobOrderFn)
	if pp.consolidationRespectPriority {
		ssn.AddConsolidationVictimFilterFn(ConsolidationVictimFilterFn)
	}
}

// ConsolidationVictimFilterFn keeps only victims of strictly lower priority than the
// consolidating job.
func ConsolidationVictimFilterFn(preemptor, victim *podgroup_info.PodGroupInfo) bool {
	return victim.Priority < preemptor.Priority
}

func JobOrderFn(l, r interface{}) int {
	lv := l.(*podgroup_info.PodGroupInfo)
	rv := r.(*podgroup_info.PodGroupInfo)

	if lv.Priority > rv.Priority {
		return -1
	}

	if lv.Priority < rv.Priority {
		return 1
	}

	return 0
}

func (pp *priorityPlugin) OnSessionClose(_ *framework.Session) {}
