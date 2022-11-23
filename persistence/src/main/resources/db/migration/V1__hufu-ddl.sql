CREATE TABLE IF NOT EXISTS `sql_record`
(
    `id`         bigint       NOT NULL AUTO_INCREMENT,
    `context`    varchar(255)      DEFAULT '',
    `user_name`  varchar(255)      DEFAULT '',
    `status`     varchar(255)      DEFAULT '',
    `start_time` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
    `end_time`   timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  CHARSET = UTF8MB4
    COMMENT 'SQL Record';

CREATE TABLE `owner`
(
    `id`                bigint       NOT NULL AUTO_INCREMENT,
    `address`           varchar(255) NOT NULL DEFAULT '',
    `user_name`         varchar(255)          DEFAULT '',
    `status`            varchar(255)          DEFAULT '',
    `tablenum`          bigint       NOT NULL DEFAULT 0,
    `local_create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `local_update_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  CHARSET = UTF8MB4
    COMMENT 'Owner';

CREATE TABLE IF NOT EXISTS `datasource`
(
    `id`                bigint        NOT NULL AUTO_INCREMENT,
    `datasource_type`   int           NOT NULL DEFAULT 0,
    `jdbc_url`          varchar(1000) NOT NULL DEFAULT '',
    `catalog`           varchar(255)  NOT NULL DEFAULT '',
    `schema`            varchar(255)  NOT NULL DEFAULT '',
    `username`          varchar(255)  NOT NULL DEFAULT '',
    `password`          varchar(255)  NOT NULL DEFAULT '',
    `local_create_time` timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `local_update_time` timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  CHARSET = UTF8MB4
    COMMENT 'DataSource Configuration';


CREATE TABLE IF NOT EXISTS `table_config`
(
    `id`                bigint       NOT NULL AUTO_INCREMENT,
    `datasource_id`     bigint       NOT NULL DEFAULT 0,
    `table_name`        varchar(255) NOT NULL DEFAULT '',
    `local_create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `local_update_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    FOREIGN KEY (`datasource_id`) REFERENCES `datasource` (`id`) ON DELETE CASCADE
) ENGINE = InnoDB
  CHARSET = UTF8MB4
    COMMENT 'Table Configuration';

