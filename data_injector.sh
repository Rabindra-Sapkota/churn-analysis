#!/bin/bash

echo "JOB STARTED AT $(date +'%Y-%m-%d %H:%M:%S')"
echo "Fetching last loaded details"
echo " "
# Get Last Loaded ID values from hive meta Table
maxLoadedProductId=$(hive -e "SELECT COALESCE(maxId, 0) from wareHouseRepo.metaTable WHERE tableName='productinfo';")
maxLoadedTxnId=$(hive -e "SELECT COALESCE(maxId, 0) from wareHouseRepo.metaTable WHERE tableName='transactioninfo';")

echo " "
echo "Starting product load from id $maxLoadedProductId and transaction load from id $maxLoadedProductId"
echo " "

# Load Product and transaction detail incrementally
productSqoop="sqoop import --table productDetail -m 10 --connect jdbc:mysql://quickstart:3306/store_detail --username=etluser --password=etlpassword --compression-codec=snappy --where \"id>$maxLoadedProductId\" --warehouse-dir=/user/hive/churnanalysis --hive-import --hive-table WareHouseRepo.dw_product --hive-overwrite"

transactionSqoop="sqoop import --table userTransaction -m 10 --connect jdbc:mysql://quickstart:3306/store_detail --username=etluser --password=etlpassword --compression-codec=snappy --where \"id>$maxLoadedTxnId\" --warehouse-dir=/user/hive/churnanalysis --hive-import --hive-table warehouserepo.dw_transaction --hive-overwrite"

echo " "
echo "Loading Product details"
echo " "
$productSqoop
echo " "
echo "Loading Transaction details"
echo " "
$transactionSqoop

# If there is no data incremental load then data will be stored as null so preserve maxloaded id in that case for next incremental load
maxImportedProductId=$(hive -e "SELECT COALESCE(MAX(id), $maxLoadedProductId, 0) FROM wareHouseRepo.dw_product;")
maxImportedTxnId=$(hive -e "SELECT COALESCE(MAX(id), $maxLoadedTxnId, 0) FROM wareHouseRepo.dw_transaction;")
currentDate=$(date +'%Y-%m-%d %H:%M:%S')

echo " "
echo "Incremental load fetched for product upto $maxImportedProductId and transaction upto $maxImportedTxnId"
echo " "
# Update meta tables
echo "Updating metatable"
echo " "

$(hive -e "INSERT OVERWRITE TABLE wareHouseRepo.metatable VALUES ('productinfo', $maxImportedProductId, '$currentDate'), ('transactioninfo', $maxImportedTxnId, '$currentDate');")

# Migrate data to destination table from temp
echo " "
echo "Migrating incremental product table to warehouse"
echo " "

$(hive -e "INSERT INTO warehouserepo.productinfo SELECT id, name FROM warehouserepo.dw_product;")

echo " "
echo "Migrating incremental transaction table to warehouse"
echo " "

$(hive -e 'INSERT INTO warehouserepo.transactionInfo SELECT t.userid, regexp_extract(transacteddate,"(^\\d{4}\-\\d{2}\-\\d{2})",1), p.productname, SUM(t.amount), COUNT(1), SUM(CAST(t.revenue AS DECIMAL(10,2))) FROM warehouserepo.dw_transaction t JOIN warehouserepo.productinfo p ON p.productid = t.productid GROUP BY t.userid, regexp_extract(transacteddate,"(^\\d{4}\-\\d{2}\-\\d{2})",1), p.productname';)

echo " "
echo "JOB ENDED AT $(date +'%Y-%m-%d %H:%M:%S')"
