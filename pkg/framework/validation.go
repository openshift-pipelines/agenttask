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
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	MaxObservationBytes        = 64 * 1024
	MaxConditionMessageBytes   = 4096
	MaxReasonBytes             = 128
	MaxResultCount             = 64
	MaxResultNameBytes         = 128
	MaxResultValueBytes        = 4096
	MaxExecutionRefFieldBytes  = 512
	MaxReferencesPerCategory   = 32
	MaxReferenceNameBytes      = 128
	MaxReferenceURIBytes       = 2048
	MaxReferenceMediaTypeBytes = 255
	MaxReferenceDigestBytes    = 255
)

var (
	digestPattern          = regexp.MustCompile(`^[a-z0-9]+(?:[+._-][a-z0-9]+)*:[a-fA-F0-9]{32,}$`)
	conditionReasonPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)
)

// ValidateObservation checks the status boundary before the framework writes it.
func ValidateObservation(observation Observation, task *agentv1alpha1.AgentTask) field.ErrorList {
	var errs field.ErrorList
	root := field.NewPath("observation")

	switch observation.State {
	case StatePending, StateAccepted, StateRunning, StateWaiting, StateSucceeded, StateFailed:
	default:
		errs = append(errs, field.Invalid(root.Child("state"), "<redacted>", "must be Pending, Accepted, Running, Waiting, Succeeded, or Failed"))
	}

	if observation.Reason != "" {
		errs = append(errs, validateBoundedString(observation.Reason, root.Child("reason"), MaxReasonBytes)...)
		if !conditionReasonPattern.MatchString(observation.Reason) {
			errs = append(errs, field.Invalid(root.Child("reason"), "<redacted>", "must begin with a letter and contain only alphanumeric characters"))
		}
	}
	errs = append(errs, validateBoundedString(observation.Message, root.Child("message"), MaxConditionMessageBytes)...)

	if requiresExecutionReference(observation.State) {
		errs = append(errs, validateExecutionReference(observation.ExecutionRef, root.Child("executionRef"))...)
	} else if observation.ExecutionRef != nil {
		errs = append(errs, validateExecutionReference(observation.ExecutionRef, root.Child("executionRef"))...)
	}

	declared := make(map[string]struct{})
	if task == nil {
		errs = append(errs, field.Required(root.Child("agentTask"), "AgentTask is required to validate results"))
	} else {
		declared = make(map[string]struct{}, len(task.Spec.Results))
		for _, result := range task.Spec.Results {
			declared[result.Name] = struct{}{}
		}
	}
	if len(observation.Results) > MaxResultCount {
		errs = append(errs, field.TooMany(root.Child("results"), len(observation.Results), MaxResultCount))
	}
	seenResults := make(map[string]struct{}, len(observation.Results))
	for i, result := range observation.Results {
		path := root.Child("results").Index(i)
		if _, ok := declared[result.Name]; !ok {
			errs = append(errs, field.Invalid(path.Child("name"), "<redacted>", "result is not declared by the AgentTask"))
		}
		if _, ok := seenResults[result.Name]; ok {
			errs = append(errs, field.Duplicate(path.Child("name"), "<redacted>"))
		}
		seenResults[result.Name] = struct{}{}
		errs = append(errs, validateBoundedString(result.Name, path.Child("name"), MaxResultNameBytes)...)
		errs = append(errs, validateBoundedString(result.Value, path.Child("value"), MaxResultValueBytes)...)
	}

	errs = append(errs, validateReferences(observation.Logs, root.Child("logs"))...)
	errs = append(errs, validateReferences(observation.Artifacts, root.Child("artifacts"))...)
	errs = append(errs, validateReferences(observation.Traces, root.Child("traces"))...)

	encoded, err := json.Marshal(observation)
	if err != nil {
		errs = append(errs, field.Invalid(root, "<redacted>", "cannot serialize observation"))
	} else if len(encoded) > MaxObservationBytes {
		errs = append(errs, field.TooLong(root, "<redacted>", MaxObservationBytes))
	}
	return errs
}

func requiresExecutionReference(state State) bool {
	switch state {
	case StateAccepted, StateRunning, StateWaiting, StateSucceeded:
		return true
	default:
		return false
	}
}

func validateExecutionReference(reference *ExecutionReference, path *field.Path) field.ErrorList {
	if reference == nil {
		return field.ErrorList{field.Required(path, "native identity is required after acceptance")}
	}
	var errs field.ErrorList
	for name, value := range map[string]string{
		"apiVersion": reference.APIVersion,
		"kind":       reference.Kind,
		"name":       reference.Name,
	} {
		fieldPath := path.Child(name)
		if value == "" {
			errs = append(errs, field.Required(fieldPath, name+" is required"))
			continue
		}
		errs = append(errs, validateBoundedString(value, fieldPath, MaxExecutionRefFieldBytes)...)
	}
	if reference.Namespace != "" {
		errs = append(errs, validateBoundedString(reference.Namespace, path.Child("namespace"), MaxExecutionRefFieldBytes)...)
		if reference.UID == "" {
			errs = append(errs, field.Required(path.Child("uid"), "server-assigned UID is required for a namespaced Kubernetes execution"))
		}
	}
	if reference.UID != "" {
		errs = append(errs, validateBoundedString(string(reference.UID), path.Child("uid"), MaxExecutionRefFieldBytes)...)
	}
	return errs
}

func validateReferences(references []Reference, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	if len(references) > MaxReferencesPerCategory {
		errs = append(errs, field.TooMany(path, len(references), MaxReferencesPerCategory))
	}
	seen := make(map[string]struct{}, len(references))
	for i, reference := range references {
		itemPath := path.Index(i)
		if reference.Name == "" {
			errs = append(errs, field.Required(itemPath.Child("name"), "name is required"))
		} else if _, ok := seen[reference.Name]; ok {
			errs = append(errs, field.Duplicate(itemPath.Child("name"), "<redacted>"))
		}
		seen[reference.Name] = struct{}{}
		errs = append(errs, validateBoundedString(reference.Name, itemPath.Child("name"), MaxReferenceNameBytes)...)

		uriErrors := validateBoundedString(reference.URI, itemPath.Child("uri"), MaxReferenceURIBytes)
		errs = append(errs, uriErrors...)
		if len(uriErrors) == 0 {
			if err := ValidateCredentialFreeURI(reference.URI); err != nil {
				errs = append(errs, field.Invalid(itemPath.Child("uri"), "<redacted>", err.Error()))
			}
		}
		if reference.MediaType != "" {
			errs = append(errs, validateBoundedString(reference.MediaType, itemPath.Child("mediaType"), MaxReferenceMediaTypeBytes)...)
		}
		if reference.Digest != "" {
			errs = append(errs, validateBoundedString(reference.Digest, itemPath.Child("digest"), MaxReferenceDigestBytes)...)
			if !digestPattern.MatchString(reference.Digest) {
				errs = append(errs, field.Invalid(itemPath.Child("digest"), "<redacted>", "must be an algorithm-prefixed hexadecimal digest"))
			}
		}
		if reference.Size != nil && *reference.Size < 0 {
			errs = append(errs, field.Invalid(itemPath.Child("size"), *reference.Size, "must be non-negative"))
		}
	}
	return errs
}

func validateBoundedString(value string, path *field.Path, maxBytes int) field.ErrorList {
	var errs field.ErrorList
	if len(value) > maxBytes {
		errs = append(errs, field.TooLong(path, "<redacted>", maxBytes))
	}
	if !utf8.ValidString(value) {
		errs = append(errs, field.Invalid(path, "<redacted>", "must be valid UTF-8"))
	}
	return errs
}

// ValidateCredentialFreeURI requires an absolute URI without embedded user
// information or common credential-bearing query parameters. Errors never
// include the URI or query values.
func ValidateCredentialFreeURI(raw string) error {
	if raw == "" {
		return errors.New("URI is required")
	}
	if !utf8.ValidString(raw) {
		return errors.New("must be valid UTF-8")
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() {
		return errors.New("must be an absolute URI")
	}
	if parsed.User != nil {
		return errors.New("must not contain user information")
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return errors.New("must contain a valid query string")
	}
	for key := range query {
		if isCredentialQueryKey(key) {
			return errors.New("must not contain credential-bearing query parameters")
		}
	}
	return nil
}

func isCredentialQueryKey(key string) bool {
	normalized := strings.ToLower(strings.NewReplacer("-", "_", ".", "_").Replace(key))
	switch normalized {
	case "access_key", "access_token", "api_key", "apikey", "auth", "authorization", "awsaccesskeyid", "bearer", "client_secret", "credential", "password", "secret", "security_token", "sig", "signature", "token", "x_amz_credential":
		return true
	}
	for _, suffix := range []string{"_token", "_secret", "_credential", "_password", "_sig", "_signature"} {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	return false
}
