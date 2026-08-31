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

package v1alpha1

import (
	"os"
	"strings"
	"testing"

	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestValidate(t *testing.T) {
	if errs := validAgentTask().Validate(); len(errs) != 0 {
		t.Fatalf("valid AgentTask returned errors: %v", errs)
	}
}

func TestValidateRejectsInvalidContract(t *testing.T) {
	task := validAgentTask()
	task.Spec.AdapterRef.Name = "not-qualified"
	task.Spec.Params = append(task.Spec.Params,
		pipelinev1.ParamSpec{Name: "request", Type: pipelinev1.ParamTypeArray},
		pipelinev1.ParamSpec{Name: "array", Type: pipelinev1.ParamTypeArray},
	)
	task.Spec.Workspaces = append(task.Spec.Workspaces, pipelinev1.WorkspaceDeclaration{Name: "source"})
	task.Spec.Results = append(task.Spec.Results, AgentTaskResult{Name: "outcome"})
	task.Spec.AdapterRef.Params = append(task.Spec.AdapterRef.Params, task.Spec.AdapterRef.Params[0])

	errs := task.Validate()
	if len(errs) < 6 {
		t.Fatalf("expected invalid selector, duplicates, and unsupported types; got %v", errs)
	}
	message := errs.ToAggregate().Error()
	for _, want := range []string{"DNS-qualified", "Duplicate value", "Unsupported value"} {
		if !strings.Contains(message, want) {
			t.Errorf("error %q does not contain %q", message, want)
		}
	}
}

func TestValidateAcceptsOmittedStringType(t *testing.T) {
	task := validAgentTask()
	task.Spec.Params[0].Type = ""
	if errs := task.Validate(); len(errs) != 0 {
		t.Fatalf("omitted string type returned errors: %v", errs)
	}
}

func TestValidateRejectsInvalidTektonDeclarations(t *testing.T) {
	task := validAgentTask()
	task.Spec.Params = []pipelinev1.ParamSpec{
		{Name: "bad/name", Type: pipelinev1.ParamTypeString},
		{Name: "object.with.dot", Type: pipelinev1.ParamTypeObject, Properties: map[string]pipelinev1.PropertySpec{}},
		{Name: "missing-properties", Type: pipelinev1.ParamTypeObject},
		{Name: "bad-default", Type: pipelinev1.ParamTypeString, Default: &pipelinev1.ParamValue{Type: pipelinev1.ParamTypeObject, ObjectVal: map[string]string{"key": "value"}}},
		{Name: "bad-property", Type: pipelinev1.ParamTypeObject, Properties: map[string]pipelinev1.PropertySpec{"bad.key": {Type: pipelinev1.ParamTypeArray}}},
		{Name: strings.Repeat("p", maxDeclarationNameBytes+1), Type: pipelinev1.ParamTypeString},
	}
	task.Spec.Workspaces[0].Name = "bad/name"
	task.Spec.Results[0].Name = "bad/name"
	task.Spec.AdapterRef.Params[0].Name = "bad/name"

	errs := task.Validate()
	if len(errs) < 8 {
		t.Fatalf("expected name, default, and object-property errors; got %v", errs)
	}
}

func TestGeneratedCRDContainsCreateTimeConstraints(t *testing.T) {
	data, err := os.ReadFile("../../config/crd/bases/agent.tekton.dev_agenttasks.yaml")
	if err != nil {
		t.Fatalf("read generated CRD: %v", err)
	}
	crd := string(data)
	if got := strings.Count(crd, "x-kubernetes-list-type: map"); got != 4 {
		t.Fatalf("generated CRD has %d map lists, want 4", got)
	}
	for _, required := range []string{
		"x-kubernetes-list-map-keys:",
		"params must use omitted/string or object types; object params",
		"pattern: ^([a-z0-9]",
		"adapter domain must not exceed 253 characters",
		"adapter name must not exceed 63 characters",
	} {
		if !strings.Contains(crd, required) {
			t.Errorf("generated CRD is missing %q", required)
		}
	}
}

func TestValidateUpdate(t *testing.T) {
	old := validAgentTask()
	metadataOnly := validAgentTask()
	metadataOnly.Labels = map[string]string{"example.com/reviewed": "true"}
	if errs := metadataOnly.ValidateUpdate(old); len(errs) != 0 {
		t.Fatalf("metadata-only update returned errors: %v", errs)
	}

	changed := validAgentTask()
	changed.Spec.AdapterRef.Name = "other.example/adapter"
	if errs := changed.ValidateUpdate(old); len(errs) == 0 {
		t.Fatal("execution-relevant spec update was accepted")
	}
}

func TestAddToScheme(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	if _, err := scheme.New(SchemeGroupVersion.WithKind("AgentTask")); err != nil {
		t.Fatalf("AgentTask is not registered: %v", err)
	}
}

func validAgentTask() *AgentTask {
	return &AgentTask{Spec: AgentTaskSpec{
		Params:     []pipelinev1.ParamSpec{{Name: "request", Type: pipelinev1.ParamTypeString}},
		Workspaces: []pipelinev1.WorkspaceDeclaration{{Name: "source"}},
		Results:    []AgentTaskResult{{Name: "outcome"}},
		AdapterRef: AgentTaskAdapterRef{
			Name: "example.com/adapter",
			Params: []pipelinev1.Param{{
				Name:  "profile",
				Value: pipelinev1.ParamValue{Type: pipelinev1.ParamTypeString, StringVal: "analysis"},
			}},
		},
	}}
}
