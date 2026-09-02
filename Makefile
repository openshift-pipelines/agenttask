CONTROLLER_GEN_VERSION := v0.17.3
CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
GENERATED := api/v1alpha1/zz_generated.deepcopy.go config/crd/bases/agent.tekton.dev_agenttasks.yaml

.PHONY: generate verify test

generate:
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases

verify:
	@test -z "$$(gofmt -l .)" || { echo "Go files need formatting"; gofmt -l .; exit 1; }
	go mod tidy -diff
	@for file in $(GENERATED); do test -f "$$file" || { echo "missing generated file: $$file"; exit 1; }; done
	@before="$$(shasum -a 256 $(GENERATED))"; \
	$(MAKE) --no-print-directory generate >/dev/null; \
	after="$$(shasum -a 256 $(GENERATED))"; \
	test "$$before" = "$$after" || { echo "generated files changed; run make generate"; exit 1; }

test:
	go test ./...
