# 迁移工具

## 功能说明

迁移工具集成了有关COS数据迁移的功能, 目前支持以下四大类迁移
- 本地数据迁移到COS, 功能同之前的本地同步工具
- 友商数据迁移到COS, 目前支持aws s3, 阿里云oss, 七牛存储, 又拍云存储
- 根据url下载列表进行下载迁移
- COS的bucket数据相互复制, 支持跨账号跨地域的数据复制

## 运行依赖
- JDK1.8或以上, 有关JDK的安装请参考[JAVA安装与配置](https://cloud.tencent.com/document/product/436/10865)
- linux或windows环境, 推荐linux

# 使用范例
1. 配置全部通过配置文件读入
sh start_migrate.sh
2. 指定部分配置项以命令行为主.
sh start_migrate.sh -DmigrateLocal.localPath=/test_data/aaa/ -Dcommon.cosPath=/aaa
sh start_migrate.sh -DmigrateAws.prefix=/test_data/bbb/ -Dcommon.cosPath=/bbb

## 迁移机制

迁移工具是有状态的，已经迁移成功的会记录在db目录下，以KV的形式存储在leveldb文件中. 
每次迁移前对要迁移的路径, 先查找下DB中是否存在, 如果存在，且属性和db中存在的一致, 则跳过迁移, 否则进行迁移。这里的属性根据迁移类型的不同而不同，对于本地迁移，会判断mtime。对于友商与bucket复制，会判断源文件的etag和长度是否与db一致。
因此，我们参照的db中是否有过迁移成功的记录，而不是查找COS，如果绕过了迁移工具，通过别的方式(比如coscmd或者控制台)删除修改了文件，那么运行迁移工具由于不会察觉到这种变化，是不会重新迁移的。

## 其他
请参照COS迁移工具[官网文档](https://cloud.tencent.com/document/product/436/15392)
