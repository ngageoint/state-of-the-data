#!/usr/bin/env bash

time spark-submit \
	--master yarn \
	--jars postgresql-42.2.14.jar \
	$1 
