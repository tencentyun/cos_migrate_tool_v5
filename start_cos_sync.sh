#!/bin/bash
export LANG=en_US.utf8

cur_dir=$(cd `dirname $0`;pwd)
cd ${cur_dir}
cp_path=${cur_dir}/src/main/resources:${cur_dir}/dep/*

java -Dfile.encoding=UTF-8 $@ -cp "$cp_path" com.qcloud.cos_migrate_tool.app.App
