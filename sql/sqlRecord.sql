/*
 Navicat Premium Data Transfer

 Source Server         : Hu-Fu
 Source Server Type    : MySQL
 Source Server Version : 50739 (5.7.39-0ubuntu0.18.04.2)
 Source Host           : localhost:3306
 Source Schema         : Hu-Fu

 Target Server Type    : MySQL
 Target Server Version : 50739 (5.7.39-0ubuntu0.18.04.2)
 File Encoding         : 65001

 Date: 16/10/2022 10:21:19
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for sqlRecord
-- ----------------------------
DROP TABLE IF EXISTS `sqlRecord`;
CREATE TABLE `sqlRecord` (
  `id` bigint(20) NOT NULL,
  `context` varchar(255) DEFAULT NULL,
  `username` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `subTime` timestamp(3) NULL DEFAULT NULL,
  `startTime` timestamp(3) NULL DEFAULT NULL,
  `execTime` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;
