CREATE DATABASE IF NOT EXISTS warehouserepo;

USE warehouserepo;


DROP TABLE IF EXISTS metatable;

CREATE TABLE metatable(
  tableName VARCHAR(255),
  maxId BIGINT,
  lastLoadedDate TIMESTAMP
);

INSERT INTO metaTable VALUES ('transactioninfo', 0, NULL), ('productinfo', 0, NULL);

DROP TABLE IF EXISTS transactioninfo;

CREATE TABLE transactioninfo(
  userid BIGINT,
  transactiondate DATE,
  productname VARCHAR(255),
  transactionamount DECIMAL(10,2),
  transactioncount int,
  revenue DECIMAL(10,2)
);

DROP TABLE IF EXISTS productinfo;

CREATE TABLE productinfo(
  productid INT,
  productname VARCHAR(255)
);

DROP TABLE IF EXISTS churn_user;

CREATE TABLE `churn_user`(
  `year` int,
  `month` int,
  `product` varchar(255),
  `userid` int,
  `past_amount` decimal(10,2),
  `past_revenue` decimal(10,2));
