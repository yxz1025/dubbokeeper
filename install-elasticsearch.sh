#!/bin/bash
mvn -Dmaven.test.skip=true clean package install -P elasticsearch assembly:assembly -U