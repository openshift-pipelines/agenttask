# AgentTask

Experimental API and Go framework for
[TEP-0170](https://github.com/tektoncd/community/pull/1263). It defines the
Pipeline-facing `AgentTask` contract and the shared `CustomRun` lifecycle used
by independently deployed adapters.

TEP-0170 remains proposed. The `agent.tekton.dev` API is unstable and this
repository provides no compatibility or product-support guarantee.

## Prototype scope

Implemented:

- namespaced `AgentTask` v1alpha1 types and generated CRD;
- declaration and invocation validation for string and object params,
  workspaces, adapter selection, and declared scalar results;
- deterministic attempt identity and a versioned, bounded
  `CustomRun.status.extraFields` profile;
- a controller-runtime reconciler embedded by one selected adapter controller;
- native execution identity immutability, credential-free references, standard
  conditions, results, timeout detection, cancellation intent, and cleanup
  finalization;
- fake-client tests for success, durable cancellation, identity changes, and
  unsafe observations.

The PoC intentionally uses one adapter installation and one active replica. It
does not yet implement distributed claiming, retries, remote resolution,
cleanup deadlines, a standalone fake controller, full conformance, Results, or
Chains integration. An uninstalled selector can therefore remain unclaimed.

## Adapter use

A Go adapter implements `framework.AgentTaskAdapter` and embeds the shared
reconciler in its controller process:

```go
framework.SetupController(manager, implementation, framework.ControllerOptions{
    Version: "poc",
    InstallationID: "example-adapter.namespace",
})
```

The framework is the only `CustomRun` status writer. The adapter creates or
adopts its native execution and returns bounded observations.

The first vertical integration is
[`agenttask-adapter-lightspeed`](https://github.com/openshift-pipelines/agenttask-adapter-lightspeed).

## Development

```sh
make generate
make verify
make test
```

Generated API artifacts are checked in and must remain reproducible with the
pinned `controller-gen` version.

## License

Apache License 2.0.
