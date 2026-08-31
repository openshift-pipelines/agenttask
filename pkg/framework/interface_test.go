/*
Copyright 2026 The AgentTask Authors

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

package framework

import (
	"context"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
)

var _ AgentTaskAdapter = stubAdapter{}

type stubAdapter struct{}

func (stubAdapter) Name(context.Context) string { return "example.com/stub" }
func (stubAdapter) Validate(context.Context, *agentv1alpha1.AgentTask, *pipelinev1beta1.CustomRun) error {
	return nil
}
func (stubAdapter) Reconcile(context.Context, Request) (Observation, error) {
	return Observation{}, nil
}
func (stubAdapter) Cancel(context.Context, Request) (Observation, error) {
	return Observation{}, nil
}
