CREATE DATABASE store_detail;
CREATE USER 'etluser'@'%' identified by 'etlpassword';
GRANT SELECT ON *.* TO 'etluser'@'%';

USE store_detail;

CREATE TABLE userTransaction(
  id BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,
  transactedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  userId INT UNSIGNED NOT NULL,
  productId INT UNSIGNED NOT NULL,
  amount DOUBLE,
  revenue DOUBLE
);

CREATE TABLE productDetail(
  id INT UNSIGNED NOT NULL auto_increment PRIMARY KEY,
  enrolledDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  name VARCHAR(500)
);

INSERT INTO productDetail(name) VALUES ('NTC Topup'), ('Ncell Topup');
INSERT INTO userTransaction(userId, productId, amount, revenue) VALUES (1, 1, 100, 5), (2, 1, 50, 2.5);
