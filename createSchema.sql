CREATE DATABASE wareHouseRepo;

USE WareHouseRepo;

CREATE TABLE metaTable(
  tableName VARCHAR(255),
  maxId bigint,
  lastLoadedDate TIMESTAMP
);

INSERT INTO metaTable VALUES ('transactionInfo', 0, NULL), ('productinfo', 0, NULL);

CREATE TABLE transactionInfo(
  userId INT,
  transactionDate DATE,
  productId INT,
  transactionAmount DECIMAL,
  revenue DECIMAL
);

CREATE TABLE productInfo(
  productId INT,
  productName VARCHAR(255)
);

