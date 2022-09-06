# OneDB

[![codecov](https://codecov.io/gh/BUAA-BDA/Hu-Fu/branch/main/graph/badge.svg?token=QJBEGGNL2P)](https://codecov.io/gh/BUAA-BDA/Hu-Fu)

## 介绍
数据库联合查询中间件

## 安装
已安装 java jdk 11 及以上版本、maven 3.5.2 及以上版本

在项目根目录下运行打包命令，打包结果位于 `release` 目录下

```
./package.sh
```

`release` 目录结构:

- bin: 可执行文件目录
    - onedb_owner_server.jar: OneDB Owner Side 可执行文件
    - onedb_user_client.jar: OneDB User Side 可执行文件
    - backend.jar: Owner && User Side 的 Spring boot 封装
- config: 配置文件目录
    - server[x].json: Owner Side 配置文件
    - client_model.json: User Side 配置文件
    - server[x].properties: Spring boot Owner Side 配置文件
    - user.properties: Spring boot User Side 配置文件
    - log4j.properties: 日志配置文件
- adapter: 数据库适配器目录
    - adapter_[x].jar: x数据库的适配器
- log: Owner Side 的日志目录
    - [x].log: owner x 的日志文件
- cert: 安全证书目录(运行本地示例时直接从 docker/cert/local 目录下拷贝如下文件到该目录即可)
    - ca.pem: CA 根证书
    - owner[x].pem: CA 签发给 owner x 的证书
    - owner[x].key: owner x 证书对应的私钥
- demo: 运行示例目录
    - config: 本地示例配置文件目录
    - start_server.sh: Owner Side 示例启动脚本
    - start_client.sh: User Side 示例启动脚本
    - stop_server.sh: Owner Side 示例关闭脚本
    - backend.sh: Spring boot 后端示例启动脚本
    - setup_env.sh: 本地示例数据库环境初始化脚本
    - shutdown_env.sh: 本地示例数据库环境关闭脚本
- user.sh: 查询端运行脚本
- owner.sh: 数据拥有端运行脚本

## 运行
`release/demo` 文件夹下提供了一个本地运行的示例，运行该示例需要完成上述安装步骤并安装 docker >= 20.10, docker-compose >= 1.29

1. 初始化数据库环境并安装证书
```
cd release/demo
./setup_env.sh
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
7. 关闭数据库
```
./shutdown_env.sh
```

## 测试

在项目根目录下运行测试命令，运行测试需要 docker >= 20.10, docker-compose >= 1.29

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
- privatekeypath: 证书私钥路径
- certchainpath: 证书路径
- trustcertpath: CA根证书路径
- adapterconfig: 数据库适配器参数
    - datasource: 数据库类型，例如 postgresql, mysql
    - url: 数据库连接 url
    - catalog: 连接的数据库
    - user: 连接用户
    - passwd: 连接密码
- tables: 预定义的可被 query user 获取的本地表 schema 信息
    - actualName: 本地真实表名
    - publishedName: 对外发布的表名，query user 通过该表名来查询此表
    - publishedColumns: 需要公开的列信息
        - name: 对外发布的列名
        - type: 对外发布的类型（当前版本需要和本地类型保持一致）
        - modifier: 安全级别
        - columnId: 对应的真实列在真实表中的序号


### UserSide

配置文件样例位于 `release/conf/client_model.json`

- owners: 需要连接的参与方信息
    - endpoint: ownerside 的 hostname:port
    - trustcertpath: 可信任的 CA 证书路径
- tables: 全局表模式信息
    - tablename: 全局表名
    - localtables: 全局表对应的本地表（可以有多个）
        - endpoint: 本地表所在 owner 的 endpoint
        - localname: 本地表的表名
