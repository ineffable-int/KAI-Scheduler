// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package reclaim_test

import (
	"fmt"
	"strings"
	"testing"

	. "go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaiv1alpha1 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/kai/v1alpha1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/eviction_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/topology_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/cache"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

// Reclaim variant of kai-scheduler/KAI-Scheduler#2426: a gang that can only run on GB200 nodes
// reclaims from an over-quota queue that also runs pods on RTX nodes. Only pods on GB200 nodes
// may be evicted; RTX pods free nothing the gang can use. The gang needs a whole rack, and the
// lowest-priority victims sit in a rack that one pod of the reclaimer's own queue keeps too
// small, so the search has to go past them. Half of each RTX node runs pods of the reclaimer's
// queue, which are not victims. On 41467076 this evicts 7 GB200 and 32 RTX pods.
func TestReclaimGangDoesNotEvictOtherPool(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	const (
		accelerator = "cloud.google.com/gke-accelerator"
		rackLabel   = "cloud.google.com/gce-topology-subblock"
	)
	nodes := map[string]nodes_fake.TestNodeBasic{}
	var jobs []*jobs_fake.TestJobBasic
	for r := 0; r < 2; r++ {
		for i := 0; i < 4; i++ {
			name := fmt.Sprintf("gb200-r%d-%d", r, i)
			nodes[name] = nodes_fake.TestNodeBasic{GPUs: 4, Labels: map[string]string{
				accelerator: "nvidia-gb200", rackLabel: fmt.Sprintf("rack%d", r)}}
			queue, priority := "batch", int32(50)
			if r == 0 {
				priority = 10
				if i == 3 {
					queue = "train"
				}
			}
			jobs = append(jobs, &jobs_fake.TestJobBasic{
				Name: "gb200-job-" + name, RequiredGPUsPerTask: 4, Priority: priority, QueueName: queue,
				JobAgeInMinutes: 1000, Tasks: []*tasks_fake.TestTaskBasic{{NodeName: name, State: pod_status.Running}},
			})
		}
	}
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("rtx-%d", i)
		nodes[name] = nodes_fake.TestNodeBasic{GPUs: 8, CPUMillis: 64, CPUMemory: 1e12, Labels: map[string]string{accelerator: "nvidia-rtx-pro-6000"}}
		for g := 0; g < 8; g++ {
			queue := "batch"
			if g >= 4 {
				queue = "train"
			}
			jobs = append(jobs, &jobs_fake.TestJobBasic{
				Name: fmt.Sprintf("rtx-job-%s-%d", name, g), RequiredGPUsPerTask: 1, Priority: 25,
				RequiredCPUsPerTask: 4, RequiredMemoryPerTask: 1e9, QueueName: queue, JobAgeInMinutes: 10,
				Tasks: []*tasks_fake.TestTaskBasic{{NodeName: name, State: pod_status.Running}},
			})
		}
	}
	root := subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName, nil)
	root.AddPodSet(subgroup_info.NewPodSet("rack", 4, &topology_info.TopologyConstraintInfo{
		Topology: "racks", RequiredLevel: rackLabel}))
	var gang []*tasks_fake.TestTaskBasic
	for i := 0; i < 4; i++ {
		gang = append(gang, &tasks_fake.TestTaskBasic{State: pod_status.Pending, SubGroupName: "rack",
			NodeSelector: map[string]string{accelerator: "nvidia-gb200"}})
	}
	jobs = append(jobs, &jobs_fake.TestJobBasic{
		Name: "reclaimer", RequiredGPUsPerTask: 4, Priority: 50, QueueName: "train", JobAgeInMinutes: 1,
		RootSubGroupSet: root, Tasks: gang,
	})

	ssn := test_utils.BuildSession(test_utils.TestTopologyBasic{
		Name: "reclaim cross pool", Nodes: nodes, Jobs: jobs,
		Topologies: []*kaiv1alpha1.Topology{{
			ObjectMeta: metav1.ObjectMeta{Name: "racks"},
			Spec:       kaiv1alpha1.TopologySpec{Levels: []kaiv1alpha1.TopologyLevel{{NodeLabel: rackLabel}}},
		}},
		Queues: []test_utils.TestQueueBasic{
			{Name: "train", DeservedGPUs: 60, GPUOverQuotaWeight: 1, ParentQueue: "root"},
			{Name: "batch", DeservedGPUs: 0, GPUOverQuotaWeight: 1, ParentQueue: "root"},
		},
		Departments: []test_utils.TestDepartmentBasic{{Name: "root", DeservedGPUs: 96}},
		Mocks: &test_utils.TestMock{
			CacheRequirements: &test_utils.CacheMocking{NumberOfPipelineActions: 100},
			SchedulerConf:     voyagerSchedulerConf(),
		},
	}, controller)

	var evicted []string
	ssn.Cache.(*cache.MockCache).EXPECT().Evict(Any(), Any(), Any(), Any()).
		DoAndReturn(func(pod *v1.Pod, _ *podgroup_info.PodGroupInfo, _ eviction_info.EvictionMetadata, _ string) error {
			evicted = append(evicted, pod.Labels["kai.scheduler/job-name"]+"/"+pod.Name)
			return nil
		}).AnyTimes()
	nodeOf := map[string]string{}
	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		for _, task := range job.GetAllPodsMap() {
			nodeOf[task.Name] = task.NodeName
		}
	}

	reclaim.New().Execute(ssn)

	placed, rtxVictims := 0, 0
	for _, task := range ssn.ClusterInfo.PodGroupInfos["reclaimer"].GetAllPodsMap() {
		if task.Status == pod_status.Pipelined {
			placed++
		}
	}
	for _, pod := range evicted {
		name := pod[strings.LastIndex(pod, "/")+1:]
		if strings.HasPrefix(nodeOf[name], "rtx-") {
			rtxVictims++
		}
	}
	t.Logf("evicted %d pods (%d on RTX nodes), reclaimer placed %d/4", len(evicted), rtxVictims, placed)
	if placed != 4 {
		t.Fatalf("reclaimer placed %d of 4 pods", placed)
	}
	// Only the 4 nodes of rack 1: rack 0 cannot fit the gang and the RTX nodes cannot run it.
	if gbVictims := len(evicted) - rtxVictims; rtxVictims != 0 || gbVictims != 4 {
		t.Errorf("evicted %d GB200 and %d RTX pods, want 4 GB200 and 0 RTX", gbVictims, rtxVictims)
	}
}

// voyagerSchedulerConf is the voyager-7 scheduler configuration (plugins and order as in the
// live snapshot), which differs from the default test configuration.
func voyagerSchedulerConf() *conf.SchedulerConfiguration {
	var plugins []conf.PluginOption
	for _, name := range []string{
		"predicates", "proportion", "priority", "nodeavailability", "resourcetype", "podaffinity", "elastic",
		"kubeflow", "ray", "subgrouporder", "taskorder", "nominatednode", "dynamicresources", "minruntime",
		"topology", "snapshot", "sg-nodelocalgreedy", "sg-multinodegang", "gpupack", "nodeplacement",
		"gpusharingorder", "backgroundpods", "numa",
	} {
		option := conf.PluginOption{Name: name}
		if name == "nodeplacement" {
			option.Arguments = map[string]string{"cpu": "binpack", "gpu": "binpack"}
		}
		plugins = append(plugins, option)
	}
	return &conf.SchedulerConfiguration{
		Actions: "allocate, reclaim, preempt, stalegangeviction",
		Tiers:   []conf.Tier{{Plugins: plugins}},
	}
}
