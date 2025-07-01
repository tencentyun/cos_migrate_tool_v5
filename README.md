# 迁移工具

## 功能说明
该迁移工具用于将本地数据一次性迁移到COS。

## 运行依赖
- JDK1.8或以上，有关JDK的安装请参考[JAVA安装与配置](https://cloud.tencent.com/document/product/436/10865)
- linux环境

## 打包方式
注意：如果需要修改源码才需要重新打包。
1. 安装maven并配置环境变量，确保maven可用
2. 进入opbin目录，直接`sh rebuild.sh`，新生成的cos_migrate_tool-x.x.x-jar-with-dependencies.jar会复制至dep目录下

# 使用范例
- 配置全部通过配置文件读入（推荐使用此方式）。进入conf目录，修改config.ini配置文件，再执行命令：
sh start_migrate.sh
- 指定部分配置项以命令行为准：
sh start_migrate.sh -DmigrateLocal.localPath=/test_data/aaa/ -Dcommon.cosPath=/aaa

## 迁移机制
- 迁移工具是有状态的，已经迁移成功的会记录在db目录下，以KV的形式存储在leveldb文件中。
- 每次迁移前对要迁移的路径，先查找下db中是否存在，如果存在，且属性和db中存在的一致，则跳过迁移，否则进行迁移，另外还会判断mtime。 因此，我们参照的db中是否有过迁移成功的记录，而不是查找COS，如果绕过了迁移工具，通过别的方式(比如coscli或者控制台)删除修改了文件，那么运行迁移工具由于不会察觉到这种变化，是不会重新迁移的。

## 其他
请参照COS迁移工具[官网文档](https://cloud.tencent.com/document/product/436/15392)
