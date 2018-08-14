#!/bin/bash
export LANG=en_US.utf8

cur_dir=$(cd `dirname $0`; cd ..; pwd)
cd ${cur_dir}
cp_path=${cur_dir}/src/main/resources:${cur_dir}/dep/*

export RUN_MODE='QUERY_REQUESTID'
export QUERY_REQUESTID_KEY="/create_test_data.sh"
echo "try to query requestid ${QUERY_REQUESID_KEY}"
java -Dfile.encoding=UTF-8 $@ -cp "$cp_path" com.qcloud.cos_migrate_tool.app.App
