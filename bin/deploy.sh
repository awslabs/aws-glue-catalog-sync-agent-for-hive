#!/bin/bash
#set -x

ver=`cat pom.xml | grep version | egrep -v "\?xml" | head -1 | cut -d">" -f2 | cut -d"<" -f1`
lib=HiveGlueCatalogSyncAgent

aws s3 cp target/$lib-$ver.jar s3://awslabs-code-us-east-1/$lib/$lib-$ver.jar --acl public-read
aws s3 cp target/$lib-$ver-complete.jar s3://awslabs-code-us-east-1/$lib/$lib-$ver-complete.jar --acl public-read
