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
	"testing"
	"time"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcilerRecordsSuccessfulObservation(t *testing.T) {
	adapter := &recordingAdapter{observation: successfulObservation()}
	reconciler, c := testReconciler(t, adapter)
	key := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "test", Name: "run"}}

	for i := 0; i < 3; i++ {
		if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
			t.Fatalf("Reconcile() call %d error = %v", i+1, err)
		}
	}

	run := getRun(t, c)
	if !run.IsSuccessful() {
		t.Fatalf("CustomRun condition = %#v, want success", run.Status.GetCondition(apis.ConditionSucceeded))
	}
	if len(run.Status.Results) != 1 || run.Status.Results[0].Name != "outcome" || run.Status.Results[0].Value != "no-action-required" {
		t.Fatalf("CustomRun results = %#v", run.Status.Results)
	}
	profile, err := DecodeStatusProfile(run)
	if err != nil {
		t.Fatalf("DecodeStatusProfile() error = %v", err)
	}
	if profile.Attempt.ID != "customrun-uid:0" || profile.ExecutionRef == nil || profile.ExecutionRef.UID != "native-uid" {
		t.Fatalf("status profile = %#v", profile)
	}
	if adapter.reconcileCalls != 1 {
		t.Fatalf("adapter Reconcile() calls = %d, want 1", adapter.reconcileCalls)
	}
}

func TestReconcilerPersistsCancellationBeforeCallingAdapter(t *testing.T) {
	adapter := &recordingAdapter{
		observation: Observation{
			State: StateAccepted,
			ExecutionRef: &ExecutionReference{
				APIVersion: "agentic.openshift.io/v1alpha1", Kind: "AgenticRun", Namespace: "test", Name: "native", UID: "native-uid",
			},
		},
		cancelObservation: Observation{State: StateCancelled, CleanupComplete: true},
	}
	reconciler, c := testReconciler(t, adapter)
	key := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "test", Name: "run"}}
	for i := 0; i < 3; i++ {
		if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
			t.Fatalf("initial Reconcile() call %d error = %v", i+1, err)
		}
	}

	run := getRun(t, c)
	run.Spec.Status = pipelinev1beta1.CustomRunSpecStatusCancelled
	if err := c.Update(context.Background(), run); err != nil {
		t.Fatalf("cancel CustomRun: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
		t.Fatalf("persist cancellation Reconcile() error = %v", err)
	}
	if adapter.cancelCalls != 0 {
		t.Fatalf("Cancel() called before intent was persisted")
	}
	persisted := getRun(t, c)
	profile, err := DecodeStatusProfile(persisted)
	if err != nil || profile.Cancellation == nil || profile.Cancellation.Reason != pipelinev1beta1.CustomRunReasonCancelled.String() {
		t.Fatalf("persisted cancellation = %#v, error = %v", profile.Cancellation, err)
	}

	if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
		t.Fatalf("cancellation Reconcile() error = %v", err)
	}
	cancelled := getRun(t, c)
	condition := cancelled.Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil || !condition.IsFalse() || condition.Reason != pipelinev1beta1.CustomRunReasonCancelled.String() {
		t.Fatalf("cancelled condition = %#v", condition)
	}
	if adapter.cancelCalls != 1 {
		t.Fatalf("Cancel() calls = %d, want 1", adapter.cancelCalls)
	}
}

func TestReconcilerPreservesWorkspaceFailureReason(t *testing.T) {
	adapter := &recordingAdapter{validateErr: ErrWorkspaceNotSupported}
	reconciler, c := testReconciler(t, adapter)
	key := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "test", Name: "run"}}
	if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	condition := getRun(t, c).Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil || condition.Reason != pipelinev1beta1.CustomRunReasonWorkspaceNotSupported.String() {
		t.Fatalf("condition = %#v", condition)
	}
}

func TestDeletionBeforeStatusInitializationRemovesFinalizer(t *testing.T) {
	adapter := &recordingAdapter{}
	run := testRun()
	now := metav1.Now()
	run.DeletionTimestamp = &now
	run.Finalizers = []string{CleanupFinalizer}
	scheme := runtime.NewScheme()
	if err := pipelinev1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pipeline scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	reconciler := &Reconciler{Client: c, Adapter: adapter}
	if _, err := reconciler.reconcileDeletion(context.Background(), run); err != nil {
		t.Fatalf("reconcileDeletion() error = %v", err)
	}
	var persisted pipelinev1beta1.CustomRun
	err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &persisted)
	if err == nil && len(persisted.Finalizers) != 0 {
		t.Fatalf("finalizers = %v, want none", persisted.Finalizers)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get deleting CustomRun: %v", err)
	}
	if adapter.cancelCalls != 0 {
		t.Fatal("adapter cancellation ran before any native execution could be created")
	}
}

func TestValidateInvocation(t *testing.T) {
	task := testTask()
	run := testRun()
	if errs := ValidateInvocation(task, run); len(errs) != 0 {
		t.Fatalf("valid invocation returned errors: %v", errs)
	}

	defaultValue := pipelinev1.ParamValue{Type: pipelinev1.ParamTypeString, StringVal: "defaulted"}
	task.Spec.Params = append(task.Spec.Params, pipelinev1.ParamSpec{Name: "optional", Type: pipelinev1.ParamTypeString, Default: &defaultValue})
	effective := ApplyParamDefaults(task, run)
	if param := effective.Spec.GetParam("optional"); param == nil || param.Value.StringVal != "defaulted" {
		t.Fatalf("defaulted param = %#v", param)
	}
	if run.Spec.GetParam("optional") != nil {
		t.Fatal("ApplyParamDefaults() mutated the original CustomRun")
	}

	run.Spec.Params = append(run.Spec.Params, pipelinev1beta1.Param{Name: "extra", Value: *pipelinev1beta1.NewStructuredValues("value")})
	if errs := ValidateInvocation(task, run); len(errs) == 0 {
		t.Fatal("undeclared param was accepted")
	}
}

func TestObjectParamDefaultsAndRequiredProperties(t *testing.T) {
	defaultValue := pipelinev1.ParamValue{Type: pipelinev1.ParamTypeObject, ObjectVal: map[string]string{"defaulted": "value"}}
	task := &agentv1alpha1.AgentTask{Spec: agentv1alpha1.AgentTaskSpec{Params: []pipelinev1.ParamSpec{{
		Name: "input", Type: pipelinev1.ParamTypeObject, Default: &defaultValue,
		Properties: map[string]pipelinev1.PropertySpec{
			"required": {Type: pipelinev1.ParamTypeString}, "defaulted": {Type: pipelinev1.ParamTypeString},
		},
	}}}}
	run := &pipelinev1beta1.CustomRun{Spec: pipelinev1beta1.CustomRunSpec{Params: []pipelinev1beta1.Param{{
		Name: "input", Value: pipelinev1beta1.ParamValue{Type: pipelinev1beta1.ParamTypeObject, ObjectVal: map[string]string{"required": "provided"}},
	}}}}
	effective := ApplyParamDefaults(task, run)
	if got := effective.Spec.Params[0].Value.ObjectVal["defaulted"]; got != "value" {
		t.Fatalf("defaulted property = %q", got)
	}
	if errs := ValidateInvocation(task, effective); len(errs) != 0 {
		t.Fatalf("effective object invocation returned errors: %v", errs)
	}
	delete(effective.Spec.Params[0].Value.ObjectVal, "required")
	if errs := ValidateInvocation(task, effective); len(errs) == 0 {
		t.Fatal("missing required object property was accepted")
	}
}

func TestStatusProfileKeepsPinnedResourceVersionAcrossMetadataUpdate(t *testing.T) {
	task := testTask()
	profile, err := NewStatusProfile(task, "example.com/adapter", "poc", "test", 0, "customrun-uid:0")
	if err != nil {
		t.Fatalf("NewStatusProfile() error = %v", err)
	}
	run := testRun()
	if err := run.Status.EncodeExtraFields(&profile); err != nil {
		t.Fatalf("EncodeExtraFields() error = %v", err)
	}
	task.ResourceVersion = "2"
	reconciler := &Reconciler{Adapter: &recordingAdapter{}, Options: ControllerOptions{Version: "poc2", InstallationID: "test"}}
	got, initialized, err := reconciler.statusProfile(task, run, "example.com/adapter", 0, "customrun-uid:0")
	if err != nil || initialized {
		t.Fatalf("statusProfile() = initialized %t, error %v", initialized, err)
	}
	if got.AgentTask.ResourceVersion != "1" || got.Adapter.Version != "poc" {
		t.Fatalf("pinned status identity = %#v", got)
	}
}

func TestStatusProfileRejectsNativeIdentityChange(t *testing.T) {
	profile := StatusProfile{ExecutionRef: &ExecutionReference{Name: "first"}}
	if err := profile.Apply(Observation{ExecutionRef: &ExecutionReference{Name: "second"}}); err == nil {
		t.Fatal("native identity change was accepted")
	}
}

type recordingAdapter struct {
	observation       Observation
	cancelObservation Observation
	validateErr       error
	reconcileCalls    int
	cancelCalls       int
}

func (*recordingAdapter) Name(context.Context) string { return "example.com/adapter" }
func (a *recordingAdapter) Validate(context.Context, *agentv1alpha1.AgentTask, *pipelinev1beta1.CustomRun) error {
	return a.validateErr
}
func (a *recordingAdapter) Reconcile(context.Context, Request) (Observation, error) {
	a.reconcileCalls++
	return a.observation, nil
}
func (a *recordingAdapter) Cancel(context.Context, Request) (Observation, error) {
	a.cancelCalls++
	return a.cancelObservation, nil
}

func getRun(t *testing.T, c client.Client) *pipelinev1beta1.CustomRun {
	t.Helper()
	var run pipelinev1beta1.CustomRun
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "test", Name: "run"}, &run); err != nil {
		t.Fatalf("get CustomRun: %v", err)
	}
	return &run
}

func testReconciler(t *testing.T, adapter *recordingAdapter) (*Reconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := pipelinev1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pipeline scheme: %v", err)
	}
	if err := agentv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add AgentTask scheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pipelinev1beta1.CustomRun{}).
		WithObjects(testTask(), testRun()).
		Build()
	return &Reconciler{
		Client:  c,
		Adapter: adapter,
		Options: ControllerOptions{
			Version: "poc", InstallationID: "test", Now: func() time.Time { return time.Unix(1_800_000_000, 0) },
		},
	}, c
}

func testTask() *agentv1alpha1.AgentTask {
	return &agentv1alpha1.AgentTask{
		TypeMeta: metav1.TypeMeta{APIVersion: agentv1alpha1.SchemeGroupVersion.String(), Kind: "AgentTask"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test", Name: "analysis", UID: types.UID("agenttask-uid"), ResourceVersion: "1",
		},
		Spec: agentv1alpha1.AgentTaskSpec{
			Params:  []pipelinev1.ParamSpec{{Name: "request", Type: pipelinev1.ParamTypeString}},
			Results: []agentv1alpha1.AgentTaskResult{{Name: "outcome"}},
			AdapterRef: agentv1alpha1.AgentTaskAdapterRef{
				Name: "example.com/adapter",
			},
		},
	}
}

func testRun() *pipelinev1beta1.CustomRun {
	return &pipelinev1beta1.CustomRun{
		TypeMeta: metav1.TypeMeta{APIVersion: pipelinev1beta1.SchemeGroupVersion.String(), Kind: "CustomRun"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test", Name: "run", UID: types.UID("customrun-uid"), ResourceVersion: "1",
		},
		Spec: pipelinev1beta1.CustomRunSpec{
			CustomRef: &pipelinev1beta1.TaskRef{APIVersion: agentv1alpha1.SchemeGroupVersion.String(), Kind: "AgentTask", Name: "analysis"},
			Params:    []pipelinev1beta1.Param{{Name: "request", Value: *pipelinev1beta1.NewStructuredValues("analyze this")}},
		},
	}
}

func successfulObservation() Observation {
	return Observation{
		State: StateSucceeded,
		ExecutionRef: &ExecutionReference{
			APIVersion: "agentic.openshift.io/v1alpha1", Kind: "AgenticRun", Namespace: "test", Name: "native", UID: "native-uid",
		},
		Results: []pipelinev1beta1.CustomRunResult{{Name: "outcome", Value: "no-action-required"}},
	}
}
