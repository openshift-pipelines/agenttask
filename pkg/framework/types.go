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
	"fmt"
	"time"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"k8s.io/apimachinery/pkg/types"
)

// State is an adapter's bounded view of native execution progress.
type State string

const (
	StatePending    State = "Pending"
	StateAccepted   State = "Accepted"
	StateRunning    State = "Running"
	StateWaiting    State = "Waiting"
	StateCancelling State = "Cancelling"
	StateCancelled  State = "Cancelled"
	StateSucceeded  State = "Succeeded"
	StateFailed     State = "Failed"
)

const (
	ReasonAgentFailed          = "AgentFailed"
	ReasonInfrastructureFailed = "InfrastructureFailed"
	ReasonCleanupFailed        = "CleanupFailed"
)

// AgentTaskAdapter maps one AgentTask CustomRun to a native backend.
type AgentTaskAdapter interface {
	Name(context.Context) string
	Validate(context.Context, *agentv1alpha1.AgentTask, *pipelinev1beta1.CustomRun) error
	Reconcile(context.Context, Request) (Observation, error)
	Cancel(context.Context, Request) (Observation, error)
}

// Request contains the immutable inputs for one execution attempt.
type Request struct {
	AgentTask          *agentv1alpha1.AgentTask
	CustomRun          *pipelinev1beta1.CustomRun
	AttemptNumber      int
	AttemptID          string
	ServiceAccountName string
	ExecutionRef       *ExecutionReference
}

// ExecutionReference identifies the authoritative native execution. A
// namespaced Kubernetes object requires its server-assigned UID. A remote
// execution may omit UID and use Name as its durable idempotency/native key.
type ExecutionReference struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace,omitempty"`
	Name       string    `json:"name"`
	UID        types.UID `json:"uid,omitempty"`
}

// Reference identifies detailed output that remains outside CustomRun status.
type Reference struct {
	Name      string `json:"name"`
	URI       string `json:"uri"`
	MediaType string `json:"mediaType,omitempty"`
	Digest    string `json:"digest,omitempty"`
	Size      *int64 `json:"size,omitempty"`
}

// Observation is the bounded state returned by an adapter reconciliation.
type Observation struct {
	State           State                             `json:"state"`
	Reason          string                            `json:"reason,omitempty"`
	Message         string                            `json:"message,omitempty"`
	ExecutionRef    *ExecutionReference               `json:"executionRef,omitempty"`
	Results         []pipelinev1beta1.CustomRunResult `json:"results,omitempty"`
	Logs            []Reference                       `json:"logs,omitempty"`
	Artifacts       []Reference                       `json:"artifacts,omitempty"`
	Traces          []Reference                       `json:"traces,omitempty"`
	RequeueAfter    time.Duration                     `json:"requeueAfter,omitempty"`
	CleanupComplete bool                              `json:"cleanupComplete,omitempty"`
}

// AttemptIdentity returns the stable idempotency key for one CustomRun attempt.
func AttemptIdentity(uid types.UID, attempt int) (string, error) {
	if uid == "" {
		return "", fmt.Errorf("CustomRun UID is required")
	}
	if attempt < 0 {
		return "", fmt.Errorf("attempt number must be non-negative")
	}
	return fmt.Sprintf("%s:%d", uid, attempt), nil
}
