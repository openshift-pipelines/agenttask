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
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentTaskSpec declares the Pipeline-visible contract and selected adapter.
// Parameters intentionally support only omitted/string and object types in this PoC.
// +kubebuilder:validation:XValidation:rule="!has(self.params) || self.params.all(p, (!has(p.type) || p.type == 'string' || p.type == 'object') && ((!has(p.type) || p.type != 'object') || has(p.properties)))",message="params must use omitted/string or object types; object params require properties"
type AgentTaskSpec struct {
	Description string `json:"description,omitempty"`

	// Params declares string or object parameters. An omitted type means string.
	// +kubebuilder:validation:MaxItems=64
	// +listType=map
	// +listMapKey=name
	Params []pipelinev1.ParamSpec `json:"params,omitempty"`

	// +kubebuilder:validation:MaxItems=64
	// +listType=map
	// +listMapKey=name
	Workspaces []pipelinev1.WorkspaceDeclaration `json:"workspaces,omitempty"`

	// +kubebuilder:validation:MaxItems=64
	// +listType=map
	// +listMapKey=name
	Results    []AgentTaskResult   `json:"results,omitempty"`
	AdapterRef AgentTaskAdapterRef `json:"adapterRef"`
}

// AgentTaskAdapterRef selects an installed adapter and implementation-specific configuration references.
type AgentTaskAdapterRef struct {
	// +kubebuilder:validation:MaxLength=317
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*/[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="self.split('/')[0].size() <= 253",message="adapter domain must not exceed 253 characters"
	// +kubebuilder:validation:XValidation:rule="self.split('/')[1].size() <= 63",message="adapter name must not exceed 63 characters"
	Name string `json:"name"`

	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	Params []pipelinev1.Param `json:"params,omitempty"`
}

// AgentTaskResult declares one scalar string result exposed through CustomRun.
type AgentTaskResult struct {
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[_A-Za-z][_A-Za-z0-9.-]*$`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=at
// +kubebuilder:storageversion

// AgentTask is an experimental reusable Custom Task definition.
type AgentTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec AgentTaskSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// AgentTaskList contains AgentTask resources.
type AgentTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentTask `json:"items"`
}
