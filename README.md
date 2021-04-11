# OneDB

## 介绍
数据库联合查询中间件

## 使用说明
已安装 java jdk 1.8及以上版本、maven 3.5.2及以上版本和 postgresql

首先在项目根目录下运行打包命令

`
./package.sh
`

修改 `release/conf/server_config.json` 中的各项配置：
- port: onedb-server 监听的端口号
- url: postgresql 的 url
- user: postgresql 登陆用户
- passwd: postgresql 对应用户密码
- catalog: 需要接入 onedb-server 的数据库名称
- tables: 需要接入 onedb-server 的表信息(可以有多张表)，每张表需要提供的信息如下
    - name: 该表表名
    - columns: 该项暂时留空

修改 `release/conf/client_model.json` 中的各项配置：
- version: 该项请勿修改
- defaultSchema: 该项请勿修改
- schemas: 可用的全局表模式
    - name: 该项请勿修改
    - type: 该项请勿修改
    - factory: 该项请勿修改
    - operands: onedb-server信息 **需要配置的项**
      - endpoints: onedb-server所在的ip及端口号
    - tables: 全局表模式信息 **需要配置的项**
        - name: 全局表名
        - factory: 该项请勿修改
        - operand: 全局到本地表映射信息
            - feds: 全局表对应的本地表（可以有多个）
                - endpoint: 本地表所在的onedb-server的 endpoint
                - name: 本地表的表名

配置完成后

```
cd release

./start_server.sh #启动onedb-server

./start_client.sh #启动onedb-client
```

需要退出 onedb-client 在命令行工具中输入

```
!q
```


需要关闭 onedb-server 时，在 `release` 目录下运行

```
./stop_server.sh
```

## zookeeper 设置说明

zookeeper 目录结构如下：

* `${ONEDB_ROOT}`
    * `${ONEDB_ROOT}/endpoint` -> null
        * `{ONEDB_ROOT}/endpoint/${endpoint_1}` -> null
            * `{ONEDB_ROOT}/endpoint/${endpoint_1}/${local_table_1}` -> header proto
            * ...
            * `{ONEDB_ROOT}/endpoint/${endpoint_1}/${local_table_n}` -> header proto
        * ...
        * `{ONEDB_ROOT}/endpoint/${endpoint_n}` -> null
    * `${ONEDB_ROOT}/schema` -> null
        * `${ONEDB_ROOT}/schema/${schema_1}` -> null
            * `${ONEDB_ROOT}/schema/${schema_1}/${global_table_1}` -> null
                * `${ONEDB_ROOT}/schema/${schema_1}/${global_table_1}/${endpoint_1}` -> local table name
                * ...
                * `${ONEDB_ROOT}/schema/${schema_1}/${global_table_1}/${endpoint_n}` -> local table name
            * ...
            * `${ONEDB_ROOT}/schema/${schema_n}/${global_table_n}` -> null
        * ...
        * `${ONEDB_ROOT}/schema/${schema_n}`
