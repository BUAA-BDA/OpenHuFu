#!/bin/bash

set -ex

if [ ! "$SONAR_TOKEN" ]; then
  echo "SONAR_TOKEN environment is null, skip check"
  exit 0
fi
mvn -B verify org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
-DskipTests \
-Dsonar.host.url=https://sonarcloud.io \
-Dsonar.organization=buaa-bda-new \
-Dsonar.projectKey=onedb \
-Dsonar.login=$SONAR_TOKEN