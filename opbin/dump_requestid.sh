#!/bin/bash
export LANG=en_US.utf8

cur_dir=$(cd `dirname $0`; cd ..; pwd)
echo ${cur_dir}
cd ${cur_dir}
cp_path=${cur_dir}/src/main/resources:${cur_dir}/dep/*

export RUN_MODE='DUMP_REQUESTID'
export DUMP_REQUESTID_FILE="${cur_dir}/dump-requestid.txt"
echo "try to dump requestid to ${DUMP_REQUESTID_FILE}"
java -Dfile.encoding=UTF-8 $@ -cp "$cp_path" com.qcloud.cos_migrate_tool.app.App
