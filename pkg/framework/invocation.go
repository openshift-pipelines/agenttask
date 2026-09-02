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
	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ApplyParamDefaults returns an invocation copy containing omitted declaration defaults.
func ApplyParamDefaults(task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) *pipelinev1beta1.CustomRun {
	if task == nil || run == nil {
		return run
	}
	effective := run.DeepCopy()
	seen := make(map[string]int, len(effective.Spec.Params))
	for i, param := range effective.Spec.Params {
		seen[param.Name] = i
	}
	for _, declared := range task.Spec.Params {
		if i, ok := seen[declared.Name]; ok {
			if declared.Default != nil && effective.Spec.Params[i].Value.Type == pipelinev1beta1.ParamTypeObject {
				if effective.Spec.Params[i].Value.ObjectVal == nil {
					effective.Spec.Params[i].Value.ObjectVal = make(map[string]string)
				}
				for key, value := range declared.Default.ObjectVal {
					if _, provided := effective.Spec.Params[i].Value.ObjectVal[key]; !provided {
						effective.Spec.Params[i].Value.ObjectVal[key] = value
					}
				}
			}
			continue
		}
		if declared.Default == nil {
			continue
		}
		paramType := declared.Default.Type
		if paramType == "" {
			paramType = declared.Type
		}
		if paramType == "" {
			paramType = pipelinev1.ParamTypeString
		}
		value := pipelinev1beta1.ParamValue{
			Type:      pipelinev1beta1.ParamType(paramType),
			StringVal: declared.Default.StringVal,
			ObjectVal: make(map[string]string, len(declared.Default.ObjectVal)),
		}
		for key, item := range declared.Default.ObjectVal {
			value.ObjectVal[key] = item
		}
		effective.Spec.Params = append(effective.Spec.Params, pipelinev1beta1.Param{Name: declared.Name, Value: value})
	}
	return effective
}

// ValidateInvocation checks the Pipeline supplied params and workspaces before
// an adapter receives them.
func ValidateInvocation(task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) field.ErrorList {
	var errs field.ErrorList
	if task == nil {
		return field.ErrorList{field.Required(field.NewPath("agentTask"), "AgentTask is required")}
	}
	if run == nil {
		return field.ErrorList{field.Required(field.NewPath("customRun"), "CustomRun is required")}
	}

	declaredParams := make(map[string]pipelinev1.ParamSpec, len(task.Spec.Params))
	for _, param := range task.Spec.Params {
		declaredParams[param.Name] = param
	}
	seenParams := make(map[string]struct{}, len(run.Spec.Params))
	for i, param := range run.Spec.Params {
		path := field.NewPath("spec", "params").Index(i)
		declared, ok := declaredParams[param.Name]
		if !ok {
			errs = append(errs, field.NotFound(path.Child("name"), param.Name))
			continue
		}
		if _, duplicate := seenParams[param.Name]; duplicate {
			errs = append(errs, field.Duplicate(path.Child("name"), param.Name))
			continue
		}
		seenParams[param.Name] = struct{}{}
		declaredType := declared.Type
		if declaredType == "" {
			declaredType = pipelinev1.ParamTypeString
		}
		if string(param.Value.Type) != string(declaredType) {
			errs = append(errs, field.Invalid(path.Child("value"), "<redacted>", "value type does not match the AgentTask declaration"))
			continue
		}
		if declaredType == pipelinev1.ParamTypeObject {
			for property := range declared.Properties {
				if _, ok := param.Value.ObjectVal[property]; !ok {
					errs = append(errs, field.Required(path.Child("value").Key(property), "required object property is missing"))
				}
			}
		}
	}
	for _, declared := range task.Spec.Params {
		if _, ok := seenParams[declared.Name]; !ok && declared.Default == nil {
			errs = append(errs, field.Required(field.NewPath("spec", "params").Key(declared.Name), "required parameter is missing"))
		}
	}

	declaredWorkspaces := make(map[string]bool, len(task.Spec.Workspaces))
	for _, workspace := range task.Spec.Workspaces {
		declaredWorkspaces[workspace.Name] = workspace.Optional
	}
	seenWorkspaces := make(map[string]struct{}, len(run.Spec.Workspaces))
	for i, workspace := range run.Spec.Workspaces {
		path := field.NewPath("spec", "workspaces").Index(i).Child("name")
		if _, ok := declaredWorkspaces[workspace.Name]; !ok {
			errs = append(errs, field.NotFound(path, workspace.Name))
			continue
		}
		if _, duplicate := seenWorkspaces[workspace.Name]; duplicate {
			errs = append(errs, field.Duplicate(path, workspace.Name))
			continue
		}
		seenWorkspaces[workspace.Name] = struct{}{}
	}
	for name, optional := range declaredWorkspaces {
		if _, ok := seenWorkspaces[name]; !ok && !optional {
			errs = append(errs, field.Required(field.NewPath("spec", "workspaces").Key(name), "required workspace is missing"))
		}
	}
	return errs
}
