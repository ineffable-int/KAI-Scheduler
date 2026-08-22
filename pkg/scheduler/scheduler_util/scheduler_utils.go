// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package scheduler_util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

func CheckNodeConditionPredicate(node *v1.Node) (bool, []string, error) {
	if node == nil {
		return false, nil, fmt.Errorf("node is nil")
	}
	reasons := []string{}

	// Unschedulable (cordoned) nodes are deliberately not filtered here: the
	// node.kubernetes.io/unschedulable taint plus the TaintToleration plugin
	// enforce cordons, which lets a pod explicitly tolerating that taint be
	// scheduled onto a cordoned node — matching upstream kube-scheduler's
	// NodeUnschedulable semantics. See kai-scheduler/KAI-Scheduler#1933.

	for _, c := range node.Status.Conditions {
		switch c.Type {
		case v1.NodeReady:
			if c.Status != v1.ConditionTrue {
				reasons = append(reasons, "node has NotReady condition")
			}
		case v1.NodeMemoryPressure,
			v1.NodeDiskPressure,
			v1.NodePIDPressure,
			v1.NodeNetworkUnavailable:

			if c.Status != v1.ConditionFalse {
				reasons = append(reasons, fmt.Sprintf("node has %s condition", c.Type))
			}
		}
	}

	return len(reasons) == 0, reasons, nil
}

func ValidateIsNodeReady(node *v1.Node) bool {
	// Cordoned nodes stay out of readiness/resource accounting even though
	// CheckNodeConditionPredicate no longer filters them for scheduling.
	if node != nil && node.Spec.Unschedulable {
		return false
	}
	ready, _, _ := CheckNodeConditionPredicate(node)
	return ready
}
