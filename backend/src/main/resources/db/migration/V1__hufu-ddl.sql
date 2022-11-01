CREATE TABLE `sql_record`
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