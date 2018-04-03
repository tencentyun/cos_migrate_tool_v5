#!/bin/bash
# 此脚本用于重新编译生成同步的JAR包
# 依赖mvn, 请确保网络畅通, 可以连接mvn仓库
cur_dir=$(cd `dirname $0`;pwd)
cd ${cur_dir}
cd ..
mvn clean compile assembly:single
mv target/*.jar dep/
rm -rf target
cd -
