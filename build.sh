#!/bin/bash

cd /GDD/costfed
export JAVA_HOME=/lib/jvm/java-11-openjdk-amd64
mvn -Dmaven.test.skip=true clean install -U

cd /GDD/sage-jena
export JAVA_HOME=/opt/jdk-20
mvn -Dmaven.test.skip=true clean install -U

cd /GDD/fedup
export JAVA_HOME=/opt/jdk-20
mvn clean install -U
