#!/bin/bash

# Make fresh directory in hadoop
echo -e "Removing /user/hive/churnanalysis directory from hadoop if exists"
hadoop fs -rm -r -f /user/hive/churnanalysis

echo -e "Creating /user/hive/churnanalysis directory in hadoop"
hadoop fs -mkdir /user/hive/churnanalysis

CURRENT_DIR=$( pwd )
#echo $CURRENT_DIR

# Load source data in MySQL(dummy data for project, not required in actual implementation)
echo -e "Creating user and schema on source mysql"
mysql -uroot -pcloudera < $CURRENT_DIR/data/mysqlSchema.sql

echo "Loading source mysql with dummy data"
mysql -uroot -pcloudera < $CURRENT_DIR/data/jan_txn_preload.sql
mysql -uroot -pcloudera < $CURRENT_DIR/data/feb_txn_preload.sql
mysql -uroot -pcloudera < $CURRENT_DIR/data/march_txn_preload.sql
mysql -uroot -pcloudera < $CURRENT_DIR/data/april_txn_incremental.sql
mysql -uroot -pcloudera < $CURRENT_DIR/data/may_txn_incremental.sql
mysql -uroot -pcloudera < $CURRENT_DIR/data/product_data.sql

# Create schema and table in hive
echo -e "Creating Schema in hive" 
hive < $CURRENT_DIR/data/hiveSchema.sql

# Give execution permission in files
echo -e "Assigning Permission to executable files"
chmod 751 *.sh *.py

# Schedure injector to pull data every month in cron
echo -e "Scheduling injector in cron to run every month"
crontab -l 2>/dev/null; echo "#1 0 1 * * $CURRENT_DIR/data_injector.sh > log/SQOOP$(date +'%Y%m%d%H%M%S').log 2>&1 &" | crontab -

# Installing python libraries
sudo python3.6 -m pip install -r $CURRENT_DIR/requirements.txt
