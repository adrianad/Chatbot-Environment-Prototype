FROM ghcr.io/open-webui/mcpo:main

USER root
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
USER $USER