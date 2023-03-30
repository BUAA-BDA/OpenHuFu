#!/bin/bash

set -ex

thread=2C

mvn --batch-mode clean deploy -T ${thread} -DskipTests