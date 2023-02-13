#!/bin/bash

set -ex

iptables -nvt filter -L INPUT
