#!/bin/bash

Hive_Query(){
     
    # Run given query on hive and return result
    RESULT=$( hive -e "$1" )
    echo $RESULT
}


Injector(){
    
    # Accepts source, incremental table name and destination table as parameter
    # Loads data from MySQL to hive incrementally
    # Incremental load in done by capturing data grated than loaded id and less than current month
    # Assumpes job is run and start of next month or some time after it but month is not missed
     
    CURRENT_MONTH_START_DATE=$( date +'%Y-%m-01')
    echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Fetching Last loaded id for $3" 
     
    MAX_LOADED_QUERY="SELECT COALESCE(maxId, 0) FROM wareHouseRepo.metaTable WHERE tableName='$3'" 
    MAX_LOADED_ID=$( Hive_Query "$MAX_LOADED_QUERY")
    echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) $3 table was loaded uptop $MAX_LOADED_ID" 
     
     
    sqoop_code="sqoop import \
       --table $1 \
       -m 100 \
       --connect jdbc:mysql://quickstart:3306/store_detail \
       --username=etluser \
       --password=etlpassword \
       --compression-codec=snappy \
       --where \"created_date<'$CURRENT_MONTH_START_DATE'\" \
       --warehouse-dir=/user/hive/churnanalysis \
       --hive-import \
       --hive-table $2 \
       --hive-overwrite \
       --direct
       --incremental append \
       --check-column id \
       --last-value $MAX_LOADED_ID"     
     
    echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Starting injection for $3"
    $sqoop_code
    echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Injection completed for $3\n"
}


Load_Churn(){

    # Load Churn data based on first day of current of month
    # First dat of month is provided via argument

    BATCH_DAY1=$1
    BATCH_MONTH_END=$(date -d "$1 -01 days + 1 month" +'%Y-%m-%d')
    BATCH_PREVIOUS_DAY1=$(date -d "$1 - 1 month" +'%Y-%m-%d')
    BATCH_PREVIOUS_MONTH_END=$(date -d "$1 -01 day" +'%Y-%m-%d')
    CHURN_YEAR=$( date -d "$BATCH_DAY1" +'%Y')
    CHURN_MONTH=$( date -d "$BATCH_DAY1" +'%m')

    echo -e "\n$(date +'%Y-%m-%d %H:%M:%S') Loading Churn for $CHURN_YEAR-$CHURN_MONTH"
    echo -e "$(date +'%Y-%m-%d %H:%M:%S') Churn date is from $BATCH_DAY1 to $BATCH_MONTH_END"
    echo -e "$(date +'%Y-%m-%d %H:%M:%S') Churn is with respect to active from $BATCH_PREVIOUS_DAY1 to $BATCH_PREVIOUS_MONTH_END"
    echo -e "$(date +'%Y-%m-%d %H:%M:%S') Loading individual churn"


    CHURN_QUERY="INSERT INTO warehouserepo.churn_user
                 SELECT
                   $CHURN_YEAR AS Year,
                   $CHURN_MONTH AS Month,
                   ta.productname AS ProductName,
                   ta.userid AS UserID,
                   SUM(ta.transactionamount) AS PastTxnAmount,
                   SUM(ta.revenue) AS PastRevenue
                 FROM warehouserepo.transactioninfo ta
                 LEFT JOIN warehouserepo.transactioninfo tin
                   ON ta.productname = tin.productname
                   AND ta.userid = tin.userid
                   AND tin.transactiondate BETWEEN '$BATCH_DAY1' AND '$BATCH_MONTH_END'
                 WHERE ta.transactiondate BETWEEN '$BATCH_PREVIOUS_DAY1' AND '$BATCH_PREVIOUS_MONTH_END'
                   AND tin.productname IS NULL
                 GROUP BY $CHURN_YEAR, $CHURN_MONTH, ta.productname, ta.userid"

    Hive_Query "$CHURN_QUERY"
     
    echo -e "$(date +'%Y-%m-%d %H:%M:%S') Loading total summary"
    Total_CHURN_QUERY="INSERT INTO warehouserepo.churn_user
                 SELECT
                   $CHURN_YEAR AS Year,
                   $CHURN_MONTH AS Month,
                   'ALL' AS ProductName,
                   ta.userid AS UserID,
                   SUM(ta.transactionamount) AS PastTxnAmount,
                   SUM(ta.revenue) AS PastRevenue
                 FROM warehouserepo.transactioninfo ta
                 LEFT JOIN warehouserepo.transactioninfo tin
                   ON ta.productname = tin.productname
                   AND ta.userid = tin.userid
                   AND tin.transactiondate BETWEEN '$BATCH_DAY1' AND '$BATCH_MONTH_END'
                 WHERE ta.transactiondate BETWEEN '$BATCH_PREVIOUS_DAY1' AND '$BATCH_PREVIOUS_MONTH_END'
                   AND tin.productname IS NULL
                 GROUP BY $CHURN_YEAR, $CHURN_MONTH,'ALL', ta.userid"
      
    Hive_Query "$Total_CHURN_QUERY"
     
     
    # echo -e "$(date +'%Y-%m-%d %H:%M:%S') Loading churn summary"

    # CHURN_SUMMARY_QUERY="INSERT INTO warehouserepo.churn_user_summary
    #                     SELECT
    #                       chu.year AS Year,
    #                       chu.month AS Month,
    #                       chu.product AS ProductName,
    #                       COUNT(DISTINCT chu.userid) AS PastUsers,
    #                       SUM(chu.past_amount) AS PastTxnAmount,
    #                       SUM(chu.past_revenue) AS PastRevenue
    #                     FROM warehouserepo.churn_user chu
    #                     WHERE month = $CHURN_MONTH AND year = $CHURN_YEAR
    #                     GROUP BY chu.year, chu.month, chu.product"

    # Hive_Query "$CHURN_SUMMARY_QUERY"
}

# Define Query, Max Loaded ID and Destination Table for ETL
# For Product
PRODUCT_SOURCE_TABLE="productDetail"
PRODUCT_INCREMENTAL_TABLE="wareHouserepo.dw_product"
PRODUCT_DESTINATION_TABLE="productinfo"

# For Transactions
TRANSACTION_SOURCE_TABLE="userTransaction "
TRANSACTION_INCREMENTAL_TABLE="warehouserepo.dw_transaction"
TRANSACTION_DESTINATION_TABLE="transactioninfo"


# If variable passed without " then space treats value as separate one.
# " Makes variable as single value solving the given problem
echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Transformation Job Started"
CURRENT_DATE=$(date +'%Y-%m-%d %H:%M:%S')

echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Starting injection"
Injector "$PRODUCT_SOURCE_TABLE" "$PRODUCT_INCREMENTAL_TABLE" "$PRODUCT_DESTINATION_TABLE"
Injector "$TRANSACTION_SOURCE_TABLE" "$TRANSACTION_INCREMENTAL_TABLE" "$TRANSACTION_DESTINATION_TABLE"


# Get Last Loaded ID values from hive meta Table
echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Getting max id from metatable for $PRODUCT_DESTINATION_TABLE"
MAX_LOADED_PRODUCT_ID=$( Hive_Query "SELECT COALESCE(maxId, 0) FROM warehouserepo.metatable WHERE tableName='$PRODUCT_DESTINATION_TABLE'")

echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Getting max id from metatable for $TRANSACTION_DESTINATION_TABLE"
MAX_LOADED_TXN_ID=$( Hive_Query "SELECT COALESCE(maxId, 0) from wareHouseRepo.metaTable WHERE tableName='$TRANSACTION_DESTINATION_TABLE'")

# If there is no data incremental load then data will be stored as null
# So preserve maxloaded id in that case for next incremental load
echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Getting max id from incremental table for $PRODUCT_INCREMENTAL_TABLE"
MAX_IMPORTED_PRODUCT_ID=$( Hive_Query "SELECT COALESCE(MAX(id), $MAX_LOADED_PRODUCT_ID) FROM $PRODUCT_INCREMENTAL_TABLE")

echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Getting max id from incremental table for $PRODUCT_INCREMENTAL_TABLE"
MAX_IMPORTED_TXN_ID=$( Hive_Query "SELECT COALESCE(MAX(id), $MAX_LOADED_TXN_ID) FROM $TRANSACTION_INCREMENTAL_TABLE")

# Update meta tables
echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Updating metatable"
Hive_Query "INSERT OVERWRITE TABLE wareHouseRepo.metatable VALUES ('$PRODUCT_DESTINATION_TABLE', $MAX_IMPORTED_PRODUCT_ID, '$CURRENT_DATE'), ('$TRANSACTION_DESTINATION_TABLE', $MAX_IMPORTED_TXN_ID, '$CURRENT_DATE')"


# Migrate data to destination table from temp
echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Migrating $PRODUCT_INCREMENTAL_TABLE to warehouse"
Hive_Query "INSERT INTO warehouserepo.productinfo SELECT id, name FROM $PRODUCT_INCREMENTAL_TABLE"

echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Migrating $PRODUCT_INCREMENTAL_TABLE to warehouse"
Hive_Query 'INSERT INTO warehouserepo.transactionInfo

            SELECT
              t.userid,
              regexp_extract(t.created_date,"(^\\d{4}\-\\d{2}\-\\d{2})",1),
              p.productname,
              SUM(t.amount),
              COUNT(1),
              SUM(CAST(t.revenue AS DECIMAL(10,2)))
            FROM warehouserepo.dw_transaction t
            JOIN warehouserepo.productinfo p
              ON p.productid = t.productid
            GROUP BY t.userid, regexp_extract(t.created_date,"(^\\d{4}\-\\d{2}\-\\d{2})",1), p.productname'

echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Transformation Job Completed"
echo -e "\n$( date +'%Y-%m-%d %H:%M:%S' ) Starting calcuration and loading churn"


echo -e "\n$( date +'%Y-%m-%d') Extracting starting month of minimum date"
MIN_DATE_QUERY='SELECT CONCAT(regexp_extract(MIN(created_date),"(^\\d{4}-\\d{2})",1),"-01") FROM warehouserepo.dw_transaction'
MIN_DATE=$( Hive_Query "$MIN_DATE_QUERY")

echo -e "\n$( date +'%Y-%m-%d') Extracting starting maximum date"
MAX_DATE_QUERY='SELECT REGEXP_EXTRACT(MAX(created_date),"(^\\d{4}-\\d{2}-\\d{2})",1) FROM warehouserepo.dw_transaction'
MAX_DATE=$( Hive_Query "$MAX_DATE_QUERY")

echo -e "\n$( date +'%Y-%m-%d') Churn Load is to be done from $MIN_DATE to $MAX_DATE"

while [[ "$MIN_DATE" < "$MAX_DATE" ]]
do
   
  Load_Churn "$MIN_DATE"
  MIN_DATE=$(date -d "$MIN_DATE + 1 month" +"%Y-%m-%d")
   
done
