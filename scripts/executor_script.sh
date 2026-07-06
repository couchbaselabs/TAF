#!/bin/bash

# Kept for backward compatibility with any caller that still sources this
# script directly. The Jenkinsfile now sources cleanup_phase.sh, install_phase.sh
# and execution_phase.sh as three separate stages instead - see those files for
# the split and why $BUILD_NUMBER replaced $$ for shared workspace filenames.
source scripts/cleanup_phase.sh
source scripts/install_phase.sh
source scripts/execution_phase.sh