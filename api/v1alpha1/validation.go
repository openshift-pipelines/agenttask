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
	"context"
	"reflect"
	"regexp"
	"strings"

	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const maxDeclarationNameBytes = 128

var (
	stringNamePattern = regexp.MustCompile(`^[_A-Za-z][_A-Za-z0-9.-]*$`)
	objectNamePattern = regexp.MustCompile(`^[_A-Za-z][_A-Za-z0-9-]*$`)
)

// Validate checks definition-level invariants. The generated CRD enforces the
// create-time subset; a future admission/controller path must invoke this
// helper for Tekton default and object-property validation.
func (task *AgentTask) Validate() field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if !strings.Contains(task.Spec.AdapterRef.Name, "/") || len(validation.IsQualifiedName(task.Spec.AdapterRef.Name)) != 0 {
		errs = append(errs, field.Invalid(specPath.Child("adapterRef", "name"), task.Spec.AdapterRef.Name, "must be a DNS-qualified name such as example.com/adapter"))
	}

	errs = append(errs, uniqueNames(task.Spec.Params, specPath.Child("params"), func(param pipelinev1.ParamSpec) string { return param.Name })...)
	for i, declared := range task.Spec.Params {
		paramPath := specPath.Child("params").Index(i)
		param := declared
		if param.Type == "" {
			param.Type = pipelinev1.ParamTypeString
		}
		if param.Type != pipelinev1.ParamTypeString && param.Type != pipelinev1.ParamTypeObject {
			errs = append(errs, field.NotSupported(paramPath.Child("type"), declared.Type, []string{string(pipelinev1.ParamTypeString), string(pipelinev1.ParamTypeObject)}))
			continue
		}
		namePattern := stringNamePattern
		if param.Type == pipelinev1.ParamTypeObject {
			namePattern = objectNamePattern
			if param.Properties == nil {
				errs = append(errs, field.Required(paramPath.Child("properties"), "object parameters require properties"))
			}
		}
		if param.Name != "" {
			if len(param.Name) > maxDeclarationNameBytes {
				errs = append(errs, field.TooLong(paramPath.Child("name"), param.Name, maxDeclarationNameBytes))
			}
			if !namePattern.MatchString(param.Name) {
				errs = append(errs, field.Invalid(paramPath.Child("name"), param.Name, "must follow Tekton parameter name syntax"))
			}
		}
		if param.Type == pipelinev1.ParamTypeObject {
			for propertyName := range param.Properties {
				if !objectNamePattern.MatchString(propertyName) {
					errs = append(errs, field.Invalid(paramPath.Child("properties").Key(propertyName), propertyName, "must follow Tekton object property name syntax"))
				}
			}
		}
		if err := param.ValidateType(context.Background()); err != nil {
			errs = append(errs, field.Invalid(paramPath, "<redacted>", "parameter default or object property types do not match the declaration"))
		}
	}

	errs = append(errs, validateNamedList(task.Spec.Workspaces, specPath.Child("workspaces"), func(workspace pipelinev1.WorkspaceDeclaration) string { return workspace.Name })...)
	errs = append(errs, validateNamedList(task.Spec.Results, specPath.Child("results"), func(result AgentTaskResult) string { return result.Name })...)
	errs = append(errs, validateNamedList(task.Spec.AdapterRef.Params, specPath.Child("adapterRef", "params"), func(param pipelinev1.Param) string { return param.Name })...)
	return errs
}

// ValidateUpdate enforces the alpha rule that execution-relevant fields are immutable.
func (task *AgentTask) ValidateUpdate(old *AgentTask) field.ErrorList {
	errs := task.Validate()
	if old != nil && !reflect.DeepEqual(task.Spec, old.Spec) {
		errs = append(errs, field.Forbidden(field.NewPath("spec"), "AgentTask spec is immutable"))
	}
	return errs
}

func validateNamedList[T any](items []T, path *field.Path, name func(T) string) field.ErrorList {
	errs := uniqueNames(items, path, name)
	for i, item := range items {
		value := name(item)
		if value != "" {
			if len(value) > maxDeclarationNameBytes {
				errs = append(errs, field.TooLong(path.Index(i).Child("name"), value, maxDeclarationNameBytes))
			}
			if !stringNamePattern.MatchString(value) {
				errs = append(errs, field.Invalid(path.Index(i).Child("name"), value, "must follow Tekton parameter name syntax"))
			}
		}
	}
	return errs
}

func uniqueNames[T any](items []T, path *field.Path, name func(T) string) field.ErrorList {
	var errs field.ErrorList
	seen := make(map[string]struct{}, len(items))
	for i, item := range items {
		value := name(item)
		namePath := path.Index(i).Child("name")
		if value == "" {
			errs = append(errs, field.Required(namePath, "name is required"))
			continue
		}
		if _, ok := seen[value]; ok {
			errs = append(errs, field.Duplicate(namePath, value))
			continue
		}
		seen[value] = struct{}{}
	}
	return errs
}
