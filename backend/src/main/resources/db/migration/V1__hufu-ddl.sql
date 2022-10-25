SET NAMES utf8mb4;

-- ----------------------------
-- Table structure for sqlRecord
-- ----------------------------
DROP TABLE IF EXISTS `sqlRecord`;
CREATE TABLE `sqlRecord` (
                             `id` bigint NOT NULL AUTO_INCREMENT,
                             `context` varchar(255) DEFAULT '',
                             `user_name` varchar(255) DEFAULT '',
                             `status` varchar(255) DEFAULT '',
                             `start_time` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
                             `end_time` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                             PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
