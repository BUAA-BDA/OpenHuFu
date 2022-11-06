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
<<<<<<< HEAD
  COMMENT 'SQL Record';

CREATE TABLE `owner`
(
    `id`         bigint       NOT NULL AUTO_INCREMENT,
    `address`    varchar(255)      DEFAULT '',
    `user_name`  varchar(255)      DEFAULT '',
    `status`     varchar(255)      DEFAULT '',
    `tablenum`   bigint            DEFAULT 0,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  CHARSET = UTF8MB4
  COMMENT 'Owner';
=======
  COMMENT 'SQL Record';
>>>>>>> ae3414e5479c87452651469bafd835e7a38674fd
