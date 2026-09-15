#!/usr/bin/env bash
# CI tool install for the nightly workflow (ubuntu-24.04 runners).
#   osmium-tool  from apt (1.16+, fine for extract / tags-filter / export)
#   rclone       from rclone.org: publish.py needs >= 1.65 for the metadata
#                REPLACE directive on server-side copies (latest/ Cache-Control).
#                Ubuntu's packaged rclone is too old.
set -euo pipefail

sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends osmium-tool

curl -fsSL --retry 3 https://rclone.org/install.sh | sudo bash >/dev/null

# sed -n 1p, not head -1: head closes the pipe after one line and the writer
# can die of SIGPIPE (exit 141), which pipefail turns into a failed step.
osmium --version | sed -n 1p
rclone version | sed -n 1p
