@echo off
set cur_dir=%CD%
cd %cur_dir%
set my_java_cp=%cur_dir%\src\main\resources;.;%cur_dir%\dep\*
java -Dfile.encoding=UTF-8 -cp "%my_java_cp%" com.qcloud.cos_migrate_tool.app.App
pause>nul
