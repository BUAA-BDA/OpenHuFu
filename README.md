# OneDB

[![coverage](https://gitlab.hufudb.com/pxc/onedb/badges/master/coverage.svg)](https://gitlab.hufudb.com/pxc/onedb)

## 介绍
数据库联合查询中间件

## 安装
已安装 java jdk 11 及以上版本、maven 3.5.2 及以上版本

在项目根目录下运行打包命令

```
./package.sh
```

## 运行
`release` 文件夹下提供了一个本地运行的示例，运行该示例需要 docker-compose 20.10 及以上版本

1. 初始化数据库环境并安装证书
```
cd release
./init_env.sh
```
2. 启动 owner side

```
./start_server.sh #启动 owner side
```

3. 启动 user side

```
./start_client.sh #启动 user side
```

4. 在命令行界面中执行查询
```
onedb>select name from student1;
```

5. 退出 user side
```
onedb>!q
```
6. 关闭 owner side
```
./stop_server.sh
```

## 测试

在项目根目录下运行测试命令，运行测试需要 docker-compose 20.10 及以上版本

```
./test.sh
```

测试覆盖率报告位于 `coverage/target/site/jacoco-aggregate/index.html`，使用清理命令清除测试相关文件

```
./clean.sh
```

## 配置文件简介

### OwnerSide

配置文件样例位于 `release/conf/server[x].json`：
- id: owner 的标识 id，一个联邦中不允许两个 owner 有相同 id
- port: owner side 监听的端口号（提供服务的端口）
- threadnum: owner side 线程池中线程数量
- privatekeypath:
- certchainpath:
- trustcertpath:
- adapterconfig: 数据库适配器参数
    - datasource: 数据库类型，例如 postgresql, mysql
    - url: 数据库连接 url
    - catalog: 连接的数据库
    - user: 连接用户
    - passwd: 连接密码
- tables: 预定义的可被 query user 获取的本地表 schema 信息
    - actualName: 本地真实表名
    - publishedName: 对外发布的表名，query user 通过该表名来查询此表
    - columns: 列模式信息，各列顺序需要和本地表一致
        - name: 对外发布的列名
        - type: 对外发布的类型（当前版本需要和本地类型保持一致）
        - modifier: 安全级别
    - actualColumns: 对应列在被发布表中的顺序，例如本地表 [A, B, C]，通过设置 actualColumns 为[2, 1, 0]，即可将发布表中列顺序变为 [C, B, A]
    - columns 和 actualColumns 同时留空则表示直接使用本地表的模式信息，并且所有列的modifier为public


### UserSide

配置文件样例位于 `release/conf/client_model.json`：
- version: 该项请勿修改
- defaultSchema: 该项请勿修改
- schemas: 可用的全局表模式
    - name: 该项请勿修改
    - type: 该项请勿修改
    - factory: 该项请勿修改
    - operand: **需要配置的项**
        - owners: 需要连接的参与方信息
            - endpoint: ownerside 的 hostname:port
            - trustcertpath: 该owner的ca证书路径
    - tables: 全局表模式信息，**需要配置的项**
        - name: 全局表名
        - factory: 该项请勿修改
        - operand: 全局到本地表映射信息
            - feds: 全局表对应的本地表（可以有多个）
                - endpoint: 本地表所在的owner的 endpoint
                - name: 本地表的表名
