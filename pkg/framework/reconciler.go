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
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const defaultRequeueAfter = 2 * time.Second

// ControllerOptions identifies one adapter installation.
type ControllerOptions struct {
	Version        string
	InstallationID string
	RequeueAfter   time.Duration
	Now            func() time.Time
}

// Reconciler maps AgentTask CustomRuns through one in-process adapter.
type Reconciler struct {
	client.Client
	Adapter AgentTaskAdapter
	Options ControllerOptions
}

func SetupController(mgr ctrl.Manager, adapter AgentTaskAdapter, options ControllerOptions) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pipelinev1beta1.CustomRun{}).
		Complete(&Reconciler{Client: mgr.GetClient(), Adapter: adapter, Options: options})
}

func (r *Reconciler) Reconcile(ctx context.Context, key ctrl.Request) (ctrl.Result, error) {
	var run pipelinev1beta1.CustomRun
	if err := r.Get(ctx, key.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !run.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, &run)
	}
	if run.IsDone() {
		return ctrl.Result{}, nil
	}

	taskName, ok := referencedAgentTask(&run)
	if !ok {
		return ctrl.Result{}, nil
	}
	var task agentv1alpha1.AgentTask
	if err := r.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: taskName}, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.markInvalid(ctx, &run, "InvalidAgentTask", "Referenced AgentTask was not found")
		}
		return ctrl.Result{}, err
	}

	adapterName := r.Adapter.Name(ctx)
	if task.Spec.AdapterRef.Name != adapterName {
		return ctrl.Result{}, nil
	}
	if errs := task.Validate(); len(errs) != 0 {
		return ctrl.Result{}, r.markInvalid(ctx, &run, "InvalidAgentTask", errs.ToAggregate().Error())
	}
	effectiveRun := ApplyParamDefaults(&task, &run)
	if errs := ValidateInvocation(&task, effectiveRun); len(errs) != 0 {
		return ctrl.Result{}, r.markInvalid(ctx, &run, "InvalidAgentTask", errs.ToAggregate().Error())
	}
	if err := r.Adapter.Validate(ctx, &task, effectiveRun); err != nil {
		reason := "InvalidAgentTask"
		message := "The selected adapter rejected this AgentTask invocation"
		if errors.Is(err, ErrWorkspaceNotSupported) {
			reason = pipelinev1beta1.CustomRunReasonWorkspaceNotSupported.String()
			message = "The selected adapter does not support these workspace bindings"
		}
		return ctrl.Result{}, r.markInvalid(ctx, &run, reason, message)
	}

	if !controllerutil.ContainsFinalizer(&run, CleanupFinalizer) {
		before := run.DeepCopy()
		controllerutil.AddFinalizer(&run, CleanupFinalizer)
		return ctrl.Result{}, r.Patch(ctx, &run, client.MergeFrom(before))
	}

	attempt := len(run.Status.RetriesStatus)
	attemptID, err := AttemptIdentity(run.UID, attempt)
	if err != nil {
		return ctrl.Result{}, err
	}
	profile, initialized, err := r.statusProfile(&task, &run, adapterName, attempt, attemptID)
	if err != nil {
		return ctrl.Result{}, r.markInvalid(ctx, &run, ReasonInfrastructureFailed, err.Error())
	}
	if initialized {
		before := run.DeepCopy()
		if run.Status.StartTime == nil {
			now := metav1.NewTime(r.now())
			run.Status.StartTime = &now
		}
		run.Status.InitializeConditions()
		run.Status.MarkCustomRunRunning("Pending", "AgentTask is ready for adapter reconciliation")
		if err := run.Status.EncodeExtraFields(&profile); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, r.Status().Patch(ctx, &run, client.MergeFrom(before))
	}

	request := Request{
		AgentTask:          &task,
		CustomRun:          effectiveRun,
		AttemptNumber:      attempt,
		AttemptID:          attemptID,
		ServiceAccountName: effectiveRun.Spec.ServiceAccountName,
		ExecutionRef:       profile.ExecutionRef,
	}
	if request.ServiceAccountName == "" {
		request.ServiceAccountName = "default"
	}

	cancellationReason := r.cancellationReason(&run)
	if profile.Cancellation != nil {
		cancellationReason = profile.Cancellation.Reason
	}
	if cancellationReason != "" {
		return r.reconcileCancellation(ctx, &run, &task, request, profile, cancellationReason)
	}

	observation, err := r.Adapter.Reconcile(ctx, request)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.recordObservation(ctx, &run, &task, profile, observation)
}

func (r *Reconciler) statusProfile(task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun, adapterName string, attempt int, attemptID string) (StatusProfile, bool, error) {
	if len(run.Status.ExtraFields.Raw) == 0 {
		profile, err := NewStatusProfile(task, adapterName, r.Options.Version, r.Options.InstallationID, attempt, attemptID)
		return profile, true, err
	}
	profile, err := DecodeStatusProfile(run)
	if err != nil {
		return StatusProfile{}, false, err
	}
	expected, err := NewStatusProfile(task, adapterName, r.Options.Version, r.Options.InstallationID, attempt, attemptID)
	if err != nil {
		return StatusProfile{}, false, err
	}
	if profile.AgentTask.APIVersion != expected.AgentTask.APIVersion ||
		profile.AgentTask.Name != expected.AgentTask.Name ||
		profile.AgentTask.UID != expected.AgentTask.UID ||
		profile.AgentTask.Digest != expected.AgentTask.Digest ||
		profile.Adapter.Name != expected.Adapter.Name ||
		profile.Adapter.InstallationID != expected.Adapter.InstallationID ||
		profile.Attempt != expected.Attempt {
		return StatusProfile{}, false, fmt.Errorf("persisted AgentTask identity does not match this invocation")
	}
	return profile, false, nil
}

func (r *Reconciler) recordObservation(ctx context.Context, run *pipelinev1beta1.CustomRun, task *agentv1alpha1.AgentTask, profile StatusProfile, observation Observation) (ctrl.Result, error) {
	if errs := ValidateObservation(observation, task); len(errs) != 0 {
		return ctrl.Result{}, fmt.Errorf("invalid adapter observation: %w", errs.ToAggregate())
	}
	if err := profile.Apply(observation); err != nil {
		return ctrl.Result{}, err
	}

	before := run.DeepCopy()
	if err := run.Status.EncodeExtraFields(&profile); err != nil {
		return ctrl.Result{}, err
	}
	message := observation.Message
	if message == "" {
		message = string(observation.State)
	}
	switch observation.State {
	case StatePending:
		run.Status.MarkCustomRunRunning("Pending", "%s", message)
	case StateAccepted:
		run.Status.MarkCustomRunRunning("Accepted", "%s", message)
	case StateRunning:
		reason := observation.Reason
		if reason == "" {
			reason = "Running"
		}
		run.Status.MarkCustomRunRunning(reason, "%s", message)
	case StateWaiting:
		run.Status.MarkCustomRunRunning("WaitingForApproval", "%s", message)
	case StateCancelling:
		run.Status.MarkCustomRunRunning("Cancelling", "%s", message)
	case StateCancelled:
		run.Status.MarkCustomRunFailed(pipelinev1beta1.CustomRunReasonCancelled.String(), "%s", message)
	case StateSucceeded:
		run.Status.Results = append([]pipelinev1beta1.CustomRunResult(nil), observation.Results...)
		run.Status.MarkCustomRunSucceeded(pipelinev1beta1.CustomRunReasonSuccessful.String(), "%s", message)
	case StateFailed:
		run.Status.MarkCustomRunFailed(observation.Reason, "%s", message)
	}
	if err := r.Status().Patch(ctx, run, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}
	if observation.State == StateSucceeded || observation.State == StateFailed || observation.State == StateCancelled {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter(observation.RequeueAfter)}, nil
}

func (r *Reconciler) reconcileCancellation(ctx context.Context, run *pipelinev1beta1.CustomRun, task *agentv1alpha1.AgentTask, request Request, profile StatusProfile, reason string) (ctrl.Result, error) {
	if profile.Cancellation == nil {
		before := run.DeepCopy()
		profile.Cancellation = &CancellationStatus{Reason: reason}
		run.Status.MarkCustomRunRunning(cancellingCondition(reason), "Native cancellation requested")
		if err := run.Status.EncodeExtraFields(&profile); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, r.Status().Patch(ctx, run, client.MergeFrom(before))
	}
	if profile.Cancellation.Reason != reason {
		return ctrl.Result{}, fmt.Errorf("cancellation reason changed from %s to %s", profile.Cancellation.Reason, reason)
	}
	observation, err := r.Adapter.Cancel(ctx, request)
	if err != nil {
		return ctrl.Result{}, err
	}
	if errs := ValidateObservation(observation, task); len(errs) != 0 {
		return ctrl.Result{}, fmt.Errorf("invalid cancellation observation: %w", errs.ToAggregate())
	}
	if err := profile.Apply(observation); err != nil {
		return ctrl.Result{}, err
	}
	before := run.DeepCopy()
	if err := run.Status.EncodeExtraFields(&profile); err != nil {
		return ctrl.Result{}, err
	}
	if observation.State == StateFailed {
		run.Status.MarkCustomRunFailed(observation.Reason, "%s", observation.Message)
		return ctrl.Result{}, r.Status().Patch(ctx, run, client.MergeFrom(before))
	}
	if observation.CleanupComplete {
		run.Status.MarkCustomRunFailed(reason, "Native execution terminated")
		return ctrl.Result{}, r.Status().Patch(ctx, run, client.MergeFrom(before))
	}
	run.Status.MarkCustomRunRunning(cancellingCondition(reason), "Waiting for native execution termination")
	if err := r.Status().Patch(ctx, run, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeueAfter(observation.RequeueAfter)}, nil
}

func (r *Reconciler) reconcileDeletion(ctx context.Context, run *pipelinev1beta1.CustomRun) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(run, CleanupFinalizer) {
		return ctrl.Result{}, nil
	}
	profile, err := DecodeStatusProfile(run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if profile.SchemaVersion == "" {
		before := run.DeepCopy()
		controllerutil.RemoveFinalizer(run, CleanupFinalizer)
		return ctrl.Result{}, r.Patch(ctx, run, client.MergeFrom(before))
	}
	if profile.Adapter.Name != r.Adapter.Name(ctx) {
		return ctrl.Result{}, nil
	}
	request := Request{CustomRun: run, AttemptNumber: profile.Attempt.Number, AttemptID: profile.Attempt.ID, ExecutionRef: profile.ExecutionRef}
	observation, err := r.Adapter.Cancel(ctx, request)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !observation.CleanupComplete {
		return ctrl.Result{RequeueAfter: r.requeueAfter(observation.RequeueAfter)}, nil
	}
	before := run.DeepCopy()
	controllerutil.RemoveFinalizer(run, CleanupFinalizer)
	return ctrl.Result{}, r.Patch(ctx, run, client.MergeFrom(before))
}

func (r *Reconciler) markInvalid(ctx context.Context, run *pipelinev1beta1.CustomRun, reason, message string) error {
	before := run.DeepCopy()
	if run.Status.StartTime == nil {
		now := metav1.NewTime(r.now())
		run.Status.StartTime = &now
	}
	run.Status.InitializeConditions()
	run.Status.MarkCustomRunFailed(reason, "%s", boundedMessage(message))
	return r.Status().Patch(ctx, run, client.MergeFrom(before))
}

func referencedAgentTask(run *pipelinev1beta1.CustomRun) (string, bool) {
	ref := run.Spec.CustomRef
	if ref == nil || ref.APIVersion != agentv1alpha1.SchemeGroupVersion.String() || string(ref.Kind) != "AgentTask" || ref.Name == "" {
		return "", false
	}
	return ref.Name, true
}

func (r *Reconciler) cancellationReason(run *pipelinev1beta1.CustomRun) string {
	if run.IsCancelled() {
		return pipelinev1beta1.CustomRunReasonCancelled.String()
	}
	if run.Status.StartTime == nil {
		return ""
	}
	timeout := run.GetTimeout()
	if timeout > 0 && !r.now().Before(run.Status.StartTime.Add(timeout)) {
		return pipelinev1beta1.CustomRunReasonTimedOut.String()
	}
	return ""
}

func cancellingCondition(reason string) string {
	if reason == pipelinev1beta1.CustomRunReasonTimedOut.String() {
		return "TimingOut"
	}
	return "Cancelling"
}

func (r *Reconciler) now() time.Time {
	if r.Options.Now != nil {
		return r.Options.Now()
	}
	return time.Now()
}

func (r *Reconciler) requeueAfter(requested time.Duration) time.Duration {
	if requested > 0 {
		return requested
	}
	if r.Options.RequeueAfter > 0 {
		return r.Options.RequeueAfter
	}
	return defaultRequeueAfter
}

func boundedMessage(message string) string {
	if !utf8.ValidString(message) {
		return "Validation failed with an invalid UTF-8 message"
	}
	if len(message) <= MaxConditionMessageBytes {
		return message
	}
	message = message[:MaxConditionMessageBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}
