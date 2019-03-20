#!/bin/bash
#set -x

ver=1.1-SNAPSHOT
lib=HiveGlueCatalogSyncAgent

aws s3 cp ../target/$lib-$ver.jar s3://awslabs-code-us-east-1/$lib/$lib-$ver.jar --acl public-read
aws s3 cp ../target/$lib-$ver-jar-with-dependencies.jar s3://awslabs-code-us-east-1/$lib/$lib-$ver-jar-with-dependencies.jar --acl public-read
