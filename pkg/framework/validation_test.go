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
	"fmt"
	"strings"
	"testing"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"k8s.io/apimachinery/pkg/types"
)

func TestAttemptIdentity(t *testing.T) {
	got, err := AttemptIdentity(types.UID("run-uid"), 2)
	if err != nil {
		t.Fatalf("AttemptIdentity() error = %v", err)
	}
	if got != "run-uid:2" {
		t.Fatalf("AttemptIdentity() = %q, want run-uid:2", got)
	}
	if _, err := AttemptIdentity("", 0); err == nil {
		t.Fatal("AttemptIdentity() accepted an empty UID")
	}
	if _, err := AttemptIdentity("run-uid", -1); err == nil {
		t.Fatal("AttemptIdentity() accepted a negative attempt")
	}
}

func TestValidateObservation(t *testing.T) {
	size := int64(42)
	observation := Observation{
		State:   StateSucceeded,
		Reason:  "Succeeded",
		Message: "analysis complete",
		ExecutionRef: &ExecutionReference{
			APIVersion: "batch/v1",
			Kind:       "Job",
			Namespace:  "ci",
			Name:       "analysis",
			UID:        "job-uid",
		},
		Results: []pipelinev1beta1.CustomRunResult{{Name: "outcome", Value: "approve"}},
		Artifacts: []Reference{{
			Name: "report", URI: "oci://registry.example/reports@sha256:abcdef", Digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: &size,
		}},
	}
	if errs := ValidateObservation(observation, definition()); len(errs) != 0 {
		t.Fatalf("valid observation returned errors: %v", errs)
	}
}

func TestValidateObservationRejectsUnsafeStatus(t *testing.T) {
	negative := int64(-1)
	refs := make([]Reference, MaxReferencesPerCategory+1)
	for i := range refs {
		refs[i] = Reference{Name: string(rune('a' + i)), URI: "https://logs.example/run"}
	}
	refs[1].Name = refs[0].Name
	refs[2].URI = "https://user:secret@logs.example/run"
	refs[3].Digest = "not-a-digest"
	refs[4].Size = &negative

	observation := Observation{
		State:   State("Unknown"),
		Message: strings.Repeat("m", MaxConditionMessageBytes+1),
		Results: []pipelinev1beta1.CustomRunResult{
			{Name: "undeclared", Value: strings.Repeat("v", MaxResultValueBytes+1)},
			{Name: "undeclared", Value: "again"},
		},
		Logs: refs,
		Artifacts: []Reference{{
			Name: "signed", URI: "https://artifacts.example/run?access_token=secret",
		}},
	}

	errs := ValidateObservation(observation, definition())
	if len(errs) < 10 {
		t.Fatalf("expected bounded-state validation errors; got %v", errs)
	}
	if strings.Contains(errs.ToAggregate().Error(), "secret") {
		t.Fatalf("validation error exposed a credential: %v", errs)
	}
}

func TestAcceptedReferenceIdentity(t *testing.T) {
	t.Run("namespaced Kubernetes object requires UID", func(t *testing.T) {
		observation := Observation{State: StateAccepted, ExecutionRef: &ExecutionReference{
			APIVersion: "batch/v1", Kind: "Job", Namespace: "ci", Name: "run",
		}}
		if errs := ValidateObservation(observation, definition()); len(errs) == 0 {
			t.Fatal("accepted namespaced execution without server-assigned UID passed validation")
		}
	})

	t.Run("namespace-less remote key is durable", func(t *testing.T) {
		observation := Observation{State: StateAccepted, ExecutionRef: &ExecutionReference{
			APIVersion: "openhands.ai/v1", Kind: "Conversation", Name: "run-uid:0",
		}}
		if errs := ValidateObservation(observation, definition()); len(errs) != 0 {
			t.Fatalf("accepted remote execution key returned errors: %v", errs)
		}
	})
}

func TestValidateObservationBoundsEveryStatusDimension(t *testing.T) {
	oversized := strings.Repeat("x", MaxExecutionRefFieldBytes+1)
	observation := Observation{
		State:  StateSucceeded,
		Reason: strings.Repeat("R", MaxReasonBytes+1),
		ExecutionRef: &ExecutionReference{
			APIVersion: oversized, Kind: "Job", Namespace: "ci", Name: "run", UID: "uid",
		},
		Results: []pipelinev1beta1.CustomRunResult{{Name: strings.Repeat("n", MaxResultNameBytes+1), Value: "value"}},
		Logs: []Reference{{
			Name: strings.Repeat("n", MaxReferenceNameBytes+1), URI: "https://logs.example/run",
			MediaType: strings.Repeat("m", MaxReferenceMediaTypeBytes+1),
			Digest:    "sha256:" + strings.Repeat("a", MaxReferenceDigestBytes),
		}},
	}
	if errs := ValidateObservation(observation, definition()); len(errs) < 6 {
		t.Fatalf("expected per-field bound errors; got %v", errs)
	}
}

func TestValidateObservationBoundsResultCount(t *testing.T) {
	task := &agentv1alpha1.AgentTask{}
	observation := Observation{State: StateSucceeded, ExecutionRef: &ExecutionReference{
		APIVersion: "batch/v1", Kind: "Job", Namespace: "ci", Name: "run", UID: "uid",
	}}
	for i := 0; i <= MaxResultCount; i++ {
		name := fmt.Sprintf("result-%d", i)
		task.Spec.Results = append(task.Spec.Results, agentv1alpha1.AgentTaskResult{Name: name})
		observation.Results = append(observation.Results, pipelinev1beta1.CustomRunResult{Name: name, Value: "value"})
	}
	if errs := ValidateObservation(observation, task); len(errs) != 1 || !strings.Contains(errs[0].Error(), "Too many") {
		t.Fatalf("expected only result-count error; got %v", errs)
	}
}

func TestValidateObservationBoundsTotalSerializedSize(t *testing.T) {
	task := &agentv1alpha1.AgentTask{}
	observation := Observation{State: StateSucceeded, ExecutionRef: &ExecutionReference{
		APIVersion: "batch/v1", Kind: "Job", Namespace: "ci", Name: "run", UID: "uid",
	}}
	for i := 0; i < MaxResultCount; i++ {
		name := fmt.Sprintf("result-%d", i)
		task.Spec.Results = append(task.Spec.Results, agentv1alpha1.AgentTaskResult{Name: name})
		observation.Results = append(observation.Results, pipelinev1beta1.CustomRunResult{Name: name, Value: strings.Repeat("v", 1024)})
	}
	if errs := ValidateObservation(observation, task); len(errs) != 1 || !strings.Contains(errs[0].Error(), "Too long") {
		t.Fatalf("expected only total serialized-size error; got %v", errs)
	}
}

func TestValidateCredentialFreeURI(t *testing.T) {
	for _, key := range []string{"api_key", "client_secret", "password", "secret", "sig", "x-amz-security-token"} {
		t.Run(key, func(t *testing.T) {
			raw := "https://artifacts.example/run?" + key + "=sensitive-value"
			err := ValidateCredentialFreeURI(raw)
			if err == nil {
				t.Fatalf("ValidateCredentialFreeURI() accepted %s", key)
			}
			if strings.Contains(err.Error(), "sensitive-value") {
				t.Fatalf("error exposed query value: %v", err)
			}
		})
	}
	if err := ValidateCredentialFreeURI("https://artifacts.example/run#access_token=sensitive-value"); err == nil {
		t.Fatal("ValidateCredentialFreeURI() accepted a fragment")
	}
	if err := ValidateCredentialFreeURI("oci://registry.example/reports@sha256:abcdef"); err != nil {
		t.Fatalf("credential-free OCI URI returned error: %v", err)
	}
}

func definition() *agentv1alpha1.AgentTask {
	return &agentv1alpha1.AgentTask{Spec: agentv1alpha1.AgentTaskSpec{
		Results: []agentv1alpha1.AgentTaskResult{{Name: "outcome"}},
	}}
}
