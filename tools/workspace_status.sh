#!/usr/bin/env bash
# Values prefixed with STABLE_ are available to rules_img templates when stamping.
if sha=$(git rev-parse --short HEAD 2>/dev/null); then
    echo "STABLE_GIT_SHA ${sha}"
fi
