# AgentTask

Experimental API and Go framework prototype for
[TEP-0170: AgentTask and Pluggable Agent Execution](https://github.com/tektoncd/community/pull/1263).
It defines the Pipeline-facing `AgentTask` contract shared by independently
deployed AgentTask Adapters.

This repository is an experimental PoC scaffold. The TEP is proposed and the
`agent.tekton.dev` API is unstable. Publication does not imply TEP acceptance,
API compatibility, or product support.

## Current slice

Implemented:

- minimal namespaced `AgentTask` v1alpha1 types and CRD;
- CRD create-time constraints for selectors, unique declaration names, and the
  intentionally narrowed string/object parameter surface;
- Go create/update helpers for additional Tekton default and object-property
  checks;
- adapter request and observation types;
- deterministic attempt identity and bounded result/reference validation.

Not implemented:

- a controller, adapter claiming, status writing, finalizers, retries, remote
  resolution, a fake adapter, or conformance tests;
- multi-installation safety or any production support guarantee.

No admission webhook or controller invokes the Go helpers yet; only constraints
present in the generated CRD are API-server enforced. This scaffold is therefore
**not TEP-0170 conformant**.

## Development

```sh
make generate
make verify
make test
```

Generated API artifacts are checked in and must remain reproducible with the
pinned `controller-gen` version in the Makefile.

## License

Apache License 2.0.
