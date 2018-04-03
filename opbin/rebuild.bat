:: 双击运行脚本, 生成的可执行程序在上一层目录的dep下
:: 此脚本用于重新编译生成迁移工具的JAR包
:: 依赖mvn, 请确保网络畅通, 可以连接mvn仓库
@echo off
set cur_dir=%CD%
cd %cur_dir%
cd ..
call mvn clean compile assembly:single
move  /Y target\*.jar dep\
rd /s /q target
echo "rebuild over!"
pause>nul
