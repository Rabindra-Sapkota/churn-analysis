CREATE DATABASE IF NOT EXiSTS store_detail;

USE store_detail;

DROP TABLE IF EXISTS productDetail;

CREATE TABLE productDetail (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `name` varchar(500),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS userTransaction;

CREATE TABLE userTransaction (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `userId` int(10) unsigned NOT NULL,
  `productId` int(10) unsigned NOT NULL,
  `amount` double DEFAULT NULL,
  `revenue` double DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=INNODB;
