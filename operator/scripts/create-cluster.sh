#!/bin/bash

echo "Running install.sh..."
source scripts/install.sh

echo "Running install-monitoring.sh..."
source scripts/install-monitoring.sh

echo "Running deploy.sh..."
source scripts/deploy.sh