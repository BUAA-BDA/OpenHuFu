# Hu-Fu

[![codecov](https://codecov.io/gh/BUAA-BDA/Hu-Fu/branch/main/graph/badge.svg?token=QJBEGGNL2P)](https://codecov.io/gh/BUAA-BDA/Hu-Fu)

A federated database middleware

## Building Hu-Fu from Source

Prerequistes:
- Linux
- Java 11
- Maven (version at least 3.5.2)

Run the following commands
```cmd
git clone https://github.com/BUAA-BDA/Hu-Fu.git
cd Hu-Fu
./package.sh
```

Hu-Fu is now installed in `release`

`release` structure:

```
release
├── adapter -- database adapter jar folder
│   └── adapter_[x].jar -- adapter of datasource [x]
├── bin -- executable file folder
│   ├── backend.jar -- Spring boot backend jar
│   ├── onedb_owner_server.jar -- Hu-Fu Owner Side jar
│   └── onedb_user_client.jar -- Hu-Fu User Side jar
├── config -- configuration file folder
│   ├── client_model.json -- User Side Configuration
│   ├── log4j.properties -- log4j Configuration
│   ├── server[x].json -- Owner Side Configuration
│   ├── server[x].properties -- Spring Boot Owner Side Configuration
│   └── user.properties -- Spring Boot User Side Configuration
├── demo -- demo folder of Hu-Fu
│   ├── backend.sh -- Spring Boot setup script
│   ├── config -- configuration files for demo
│   ├── setup_env.sh -- multiple databases setup script
│   ├── shutdown_env.sh -- databases shutdown script
│   ├── start_client.sh -- User Side setup script
│   ├── start_server.sh -- Owner Side demo setup script
│   └── stop_server.sh -- Owner Side demo shutdown script
├── owner.sh -- Owner Side setup script
├── udf -- User define function jar folder
└── user.sh -- User Side setup script
```

## Running Hu-Fu (demo)

Prerequistes:
- docker (version at least 20.10)
- docker-compose (version at least 1.29)

The demo of Hu-Fu is placed in `release/demo` (need to build from source first)

1. initialize multiple database environments
```
cd release/demo
./setup_env.sh
```
2. setup the owner side of Hu-Fu

```
./start_server.sh
```

3. setup the user side of Hu-Fu

```
./start_client.sh
```

4. execute queries in the command line interface
```
onedb>select name from student1;
```

5. quit the user side of Hu-Fu
```
onedb>!q
```
6. shutdown the owner side of Hu-Fu
```
./stop_server.sh
```
7. shutdown databases
```
./shutdown_env.sh
```

## Running Hu-Fu test

Prerequistes:
- docker (version at least 20.10)
- docker-compose (version at least 1.29)


```
./test.sh
```

Test result are placed `coverage/target/site/jacoco-aggregate`, use the following commands to clean up the test realated files

```
./clean.sh
```

## Basic Introduction for Configuration File

### OwnerSide

The configuration is located in `release/conf/` as `server[x].json`, the details are as follows:
- id: the identifier of the owner, two different owners are not allowed to have the same id in a federation
- hostname: the hostname of the owner which is accessible by other owners and query users
- port: the port on which the owner side listens
- threadnum: owner side thread pool size
- adapterconfig: database adapter configurations
    - datasource: database system type, such as postgresql, mysql
    - url: connection url
    - catalog: database to connect
    - user: user name
    - passwd: password of the user
- tables: predefined local table schemas that can be obtained by query user
    - actualName: the actual name of the table in the database system
    - publishedName: the table name published to the outside, query users can use the name to query this table
    - publishedColumns: columns informations
        - name: the column name published to the outside
        - type: the column type published to the outside (for now, it needs to be consistent with the actual type)
        - modifier: security level(public, protected and hidden)
        - columnId: the ordinal number of the corresponding column in the actual table


### UserSide

The configuration is located in `release/conf/client_model.json`

- owners: the owner information
    - endpoint: {hostname}:{port} of the corresponding owner side
- tables: global table configuration
    - tablename: global table name
    - localtables: components of the global table (at least one)
        - endpoint: the endpoint of the owner of the local table
        - localname: published name of the table
