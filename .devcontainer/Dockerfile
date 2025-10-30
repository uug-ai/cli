FROM mcr.microsoft.com/devcontainers/go:1.24-bookworm

# Install Node.js and npm
RUN apt-get update && \
    apt-get install -y nodejs npm && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER vscode

# Install Go tools
RUN go install github.com/swaggo/swag/cmd/swag@latest
RUN go install github.com/jfeliu007/goplantuml/cmd/goplantuml@latest
