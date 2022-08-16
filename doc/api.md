# API 文档

## API 分类

Hu-Fu API 分为两类，查询用户端(user)和数据拥有端(owner)

用户端API主要功能包括：增删查服务端，增删改查全局表，执行联邦查询

服务端API主要功能包括：查看本地表，增删虚拟表

## 用户端 API

| 路径 | 方法 | 参数 | 返回 | 描述 |
| --- | --- | --- | --- | --- |
| /user/endpoints | GET | / | [endpoint1, .., endpointsn] | 获取所有服务端 endpoints |
| /user/endpoints | POST | endpoint | true/false | 增加服务端 |
| /user/endpoints/{endpoint} | DEL | / | / | 删除服务端 |
| /user/endpoints/{endpoint} | GET | / | [localtableinfo1, ..., localtableinfon] | 获取服务端所有虚拟表信息 |
| /user/globaltables | GET | / | [globaltableinfo1, ..., globaltableinfon] | 获取所有全局表信息 |
| /user/globaltables/{name} | GET | / | [globaltableinfo1, ..., globaltableinfon] | 获取指定名称全局表信息 |
| /user/globaltables | POST | tablemeta | true/false | 创建全局表 |
| /user/globaltables/{name} | DEL | / | / | 删除指定名称全局表 |
| /user/query | POST | sql | resultset | 执行联邦查询 |

## 服务端 API

| 路径 | 方法 | 参数 | 返回 | 描述 |
| --- | --- | --- | --- | --- |
| /owner/localtables | GET | / | [localtableinfo1, ..., localtableinfon] | 获取所有服务端所有本地表 |
| /owner/publishedtables | GET | / | [localtableinfo1, ..., localtableinfon] | 获取所有虚拟表 |
| /owner/publishedtables| POST | publishedtableinfo | true/false | 创建虚拟表 |
| /owner/publishedtables/{name} | DEL | / | void | 删除指定名称虚拟表 |

