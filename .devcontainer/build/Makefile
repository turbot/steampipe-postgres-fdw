VERSION ?= 1.0.0

build:
	docker buildx build --push --platform linux/arm64,linux/amd64 \
		--tag ghcr.io/turbot/steampipe-postgres-fdw-devcontainer:$(VERSION) \
		--build-arg DEVCONTAINER_VERSION=$(VERSION) \
		.

build-local:
	docker build --tag steampipe-postgres-fdw-devcontainer:$(VERSION) \
		--build-arg DEVCONTAINER_VERSION=$(VERSION) \
		.

test: build-local
	docker run --rm steampipe-postgres-fdw-devcontainer:$(VERSION) /bin/bash -c "\
		echo 'Testing dev container:' && \
		go version && \
		pg_config --version && \
		gcc --version | head -n1 && \
		echo 'All tools available!'" 