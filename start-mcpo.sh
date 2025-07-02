#!/bin/bash

# Create output directory if it doesn't exist
mkdir -p output

# Start mcpo in background with output logging
nohup uvx --with r2r --with mcp mcpo --port 8005 --config mcpo-config.json > output/mcpo.txt 2>&1 &

echo "MCPO started in background. Check output/mcpo.txt for logs."
