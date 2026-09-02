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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
)

const (
	StatusSchemaVersion = "agent.tekton.dev/v1alpha1"
	CleanupFinalizer    = "agent.tekton.dev/native-cleanup"
)

// StatusProfile is the bounded AgentTask lifecycle data stored in
// CustomRun.status.extraFields.
type StatusProfile struct {
	SchemaVersion string              `json:"schemaVersion"`
	AgentTask     DefinitionIdentity  `json:"agentTask"`
	Adapter       AdapterIdentity     `json:"adapter"`
	Attempt       AttemptStatus       `json:"attempt"`
	ExecutionRef  *ExecutionReference `json:"executionRef,omitempty"`
	Logs          []Reference         `json:"logs,omitempty"`
	Artifacts     []Reference         `json:"artifacts,omitempty"`
	Traces        []Reference         `json:"traces,omitempty"`
	Cancellation  *CancellationStatus `json:"cancellation,omitempty"`
}

type DefinitionIdentity struct {
	APIVersion      string `json:"apiVersion"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
	Digest          string `json:"digest"`
}

type AdapterIdentity struct {
	Name           string `json:"name"`
	Version        string `json:"version,omitempty"`
	InstallationID string `json:"installationID,omitempty"`
}

type AttemptStatus struct {
	Number int    `json:"number"`
	ID     string `json:"id"`
}

type CancellationStatus struct {
	Reason string `json:"reason"`
}

func NewStatusProfile(task *agentv1alpha1.AgentTask, adapterName, adapterVersion, installationID string, attempt int, attemptID string) (StatusProfile, error) {
	digest, err := DefinitionDigest(task)
	if err != nil {
		return StatusProfile{}, err
	}
	return StatusProfile{
		SchemaVersion: StatusSchemaVersion,
		AgentTask: DefinitionIdentity{
			APIVersion:      agentv1alpha1.SchemeGroupVersion.String(),
			Name:            task.Name,
			UID:             string(task.UID),
			ResourceVersion: task.ResourceVersion,
			Digest:          digest,
		},
		Adapter: AdapterIdentity{Name: adapterName, Version: adapterVersion, InstallationID: installationID},
		Attempt: AttemptStatus{Number: attempt, ID: attemptID},
	}, nil
}

func DefinitionDigest(task *agentv1alpha1.AgentTask) (string, error) {
	if task == nil {
		return "", fmt.Errorf("AgentTask is required")
	}
	data, err := json.Marshal(task.Spec)
	if err != nil {
		return "", fmt.Errorf("serialize AgentTask spec: %w", err)
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func DecodeStatusProfile(run *pipelinev1beta1.CustomRun) (StatusProfile, error) {
	var profile StatusProfile
	if run == nil || len(run.Status.ExtraFields.Raw) == 0 {
		return profile, nil
	}
	if err := run.Status.DecodeExtraFields(&profile); err != nil {
		return StatusProfile{}, fmt.Errorf("decode AgentTask status: %w", err)
	}
	if profile.SchemaVersion != StatusSchemaVersion {
		return StatusProfile{}, fmt.Errorf("unsupported AgentTask status schema %q", profile.SchemaVersion)
	}
	return profile, nil
}

func (profile *StatusProfile) Apply(observation Observation) error {
	if observation.ExecutionRef != nil {
		if profile.ExecutionRef != nil && *profile.ExecutionRef != *observation.ExecutionRef {
			return fmt.Errorf("native execution identity changed")
		}
		reference := *observation.ExecutionRef
		profile.ExecutionRef = &reference
	}
	profile.Logs = append([]Reference(nil), observation.Logs...)
	profile.Artifacts = append([]Reference(nil), observation.Artifacts...)
	profile.Traces = append([]Reference(nil), observation.Traces...)
	return nil
}
