
VERSION=unknown

TOPDIR=$(shell git rev-parse --show-toplevel)

GOFILES=cassandra

all: godeps build container

build: ${GOFILES}

${GOFILES}: %: %.go
	GOPATH=${TOPDIR} go build $<

godeps:
	GOPATH=${TOPDIR} ${TOPDIR}/bin/dep ensure

container: $(subst Dockerfile.,container.,$(wildcard Dockerfile.*[a-z]))

# Below, the container.% target creates a .dockerignore file appropriate
# for the container.  This includes the name of the executable, but also
# includes stuff from the *_INCLUDES macro.  At the moment, geoip is the
# only container which needs extra things included, specifically the GeoIP.conf
# configuration file.  This prevents loading 200M of stuff into the build.

container.%: CONTAINER=$(@:container.%=%)
container.%: Dockerfile.% %
	docker build -t \
		gcr.io/trust-networks/analytics-${CONTAINER}:${VERSION} \
	 	-f Dockerfile.${CONTAINER} .

.PHONY: push
push: $(subst Dockerfile.,push.,$(wildcard Dockerfile.*[a-z]))

push.%: CONTAINER=$(@:push.%=%)
push.%:
	gcloud docker -- push \
	  gcr.io/trust-networks/analytics-${CONTAINER}:${VERSION}

clean:
	git clean -Xffdi

version:
	@echo ${VERSION}

tag:
	git tag -a v${VERSION} -m ''
	git push --tags
