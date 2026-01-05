FROM mcr.microsoft.com/devcontainers/go:1.24-bookworm AS builder

ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$GOROOT/bin:/usr/local/lib:$PATH
ENV GOSUMDB=off
ENV GOTOOLCHAIN=auto

ARG project
ARG github_username
ARG github_token

##############################################################################
# Copy all the relevant source code in the Docker image, so we can build this.

RUN git config --global \
    url."https://${github_username}:${github_token}@github.com/".insteadOf \
    "https://github.com/"

##########################################
# Installing some additional dependencies.

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential cmake pkg-config unzip libgtk2.0-dev \
    curl ca-certificates libcurl4-openssl-dev libssl-dev libjpeg62-turbo-dev && \
    rm -rf /var/lib/apt/lists/*

##############################################################################
# Copy all the relevant source code in the Docker image, so we can build this.

RUN mkdir -p /go/src/github.com/uug-ai/${project}
COPY . /go/src/github.com/uug-ai/${project}

##################
# Build Project

RUN cd /go/src/github.com/uug-ai/${project} && \
    go mod download && \
    go build -tags timetzdata,netgo --ldflags '-s -w -extldflags "-static -latomic"' main.go && \
    mkdir -p /${project} && \
    mv main /${project} && \
    mv indexes /${project}/indexes && \
    rm -rf /go/src/github.com/uug-ai/${project}

####################################
# Let's create a /dist folder containing just the files necessary for runtime.
# Later, it will be copied as the / (root) of the output image.

WORKDIR /dist
RUN cp -r /${project} ./

##############################
# Final Stage: Create the small runtime image.

FROM alpine:latest
LABEL org.opencontainers.image.source https://github.com/uug-ai/cli
LABEL AUTHOR=uug-ai

ARG project

COPY --chown=0:0 --from=builder /${project} /

ENTRYPOINT ["/main"]
