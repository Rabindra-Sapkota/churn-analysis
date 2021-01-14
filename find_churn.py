#!/usr/local/bin/python3.6

from pyhive import hive
import pandas as pd
import mailerpy as mailer


def read_hive_data(query):
    """
    :param query: Query to be run in hive
    :type query: str
    """
     
    con=hive.Connection(host="localhost", port=10000, database="warehouserepo")
    df=pd.read_sql(query, con)
    return df


def save_as_csv(df,file_name):
    """
    :param df: Dataframe to save as csv
    :type df: Pandas Dataframe
    :param file_name: Name of dumped file
    :type df: str
    """
     
    df.to_csv('/home/rabindra/chrun-analysis/report/processing/' + file_name, index=False) 
    return 1

'''
class dataframes:
    def __init__(query):
        self.query=query
     
     
    def generate_dataframe(self):
        con=hive.Connection(host="localhost", port=10000, database="warehouserepo")
        self.df=pd.read_sql(self.query, con)
     
     
    def save_as_csv(self, file_name):
        self.df.to_csv('/home/rabindra/chrun-analysis/report/processing/' + file_name, index=False)    
     
     
    def save_fig(self):
        # Save analysis in figure
        return


def main():
    # active_from_date=input("Enter active from date(YYYY-MM-DD): ")
    # active_to_date=input("Enter active to date(YYYY-MM-DD): ")
    # inactive_from_date=input("Enter churn from date(YYYY-MM-DD): ")
    # inactive_to_date=input("Enter churn to date(YYYY-MM-DD): ")
     
     
    user_churn_query="""
    SELECT
      ta.userid AS UserID,
      SUM(ta.transactioncount) AS PastTxnCount,
      SUM(ta.transactionamount) AS PastTxnAmount,
      SUM(ta.revenue) AS PastRevenue
    FROM transactioninfo ta
    LEFT JOIN transactioninfo tin
      ON ta.userid = tin.userid
      AND tin.transactiondate BETWEEN '2021-01-10' AND '2021-01-10'
    WHERE ta.transactiondate BETWEEN '2021-01-09' AND '2021-01-09'
      AND tin.userid IS NULL
    GROUP BY ta.userid
    """.format(inactive_from_date, inactive_to_date, active_from_date, active_to_date)
     
    print(user_churn_query)
     
    user_product_churn_query="""
    SELECT
      ta.userid AS UserID,
      ta.productname AS ProductName,
      COUNT(DISTINCT ta.userid) AS PastUsers,
      SUM(ta.transactioncount) AS PastTxnCount,
      SUM(ta.transactionamount) AS PastTxnAmount,
      SUM(ta.revenue) AS PastRevenue
    FROM transactioninfo ta
    LEFT JOIN transactioninfo tin
      ON ta.productname = tin.productname
      AND ta.userid = tin.userid
      AND tin.transactiondate BETWEEN '2021-01-10' AND '2021-01-10'
    WHERE ta.transactiondate BETWEEN '2021-01-09' AND '2021-01-09'
      AND tin.productname IS NULL
    GROUP BY ta.userid, ta.productname;
    """.format(inactive_from_date, inactive_to_date, active_from_date, active_to_date)
     
    print(user_product_churn_query)
     
    product_churn_query="""
    SELECT
      ta.productname AS ProductName,
      COUNT(DISTINCT ta.userid) AS PastUsers,
      SUM(ta.transactioncount) AS PastTxnCount,
      SUM(ta.transactionamount) AS PastTxnAmount,
      SUM(ta.revenue) AS PastRevenue
    FROM transactioninfo ta
    LEFT JOIN transactioninfo tin
      ON ta.productname = tin.productname
      AND tin.transactiondate BETWEEN '2021-01-10' AND '2021-01-10'
    WHERE ta.transactiondate BETWEEN '2021-01-09' AND '2021-01-09'
      AND tin.productname IS NULL
    GROUP BY ta.productname;
    """.format(inactive_from_date, inactive_to_date, active_from_date, active_to_date)
     
    print(product_churn_query)
     
    user_churn_df=read_hive_data(user_churn_query)
    print(user_churn_df)
    save_as_csv(user_churn_df, 'test')
'''


def get_churn_trend(product_name):
    """
    :product_code: product whose churn trend is to be found
    :type product_name: str
    """
    
    query="""SELECT
               year,
               month,
               product,
               userid,
               past_amount,
               past_revenue
             FROM churn_user
             WHERE lcase(product) = lcase('{}')
          """.format(product_name) 
         
    churn_trend_df = read_hive_data(query)
     
    churn_trend_aggregated = churn_trend_df.groupby(['year', 'month', 'product'])[
          ['userid', 'past_amount', 'past_revenue']].agg(
          {'userid':'nunique', 'past_amount':'sum', 'past_revenue':'sum'}).reset_index()
     
    return churn_trend_aggregated


def get_churn_info_month(year, month):
    """
    :year: Year for which churn info is to be calculated
    :month: Month for which churn info is to be calculated 
    """
     
    query="""SELECT
               year,
               month,
               product,
               userid,
               past_amount,
               past_revenue
             FROM churn_user
             WHERE year={} AND month={} AND product <> 'ALL'
          """.format(year, month)  
     
    churn_info_month = read_hive_data(query)
     
    churn_info_grouped = churn_info_month.groupby(['year', 'month', 'product'])[
            ['userid', 'past_amount', 'past_revenue']].agg(
            {'userid':'nunique', 'past_amount':'sum', 'past_revenue':'sum'}).reset_index()
     
    churn_info_grouped = churn_info_grouped.sort_values(['past_revenue', 'past_amount'], ascending=False).reset_index()
    churn_info_grouped.loc[churn_info_grouped.index >=5, 'product'] = 'Other'
    result_df = churn_info_grouped.groupby(['year', 'month', 'product'])[
             ['userid', 'past_amount', 'past_revenue']].agg(
             {'userid':'nunique', 'past_amount':'sum', 'past_revenue':'sum'}).reset_index()
     
    result_df = result_df.sort_values(['past_revenue', 'past_amount'], ascending=False).reset_index(drop=True)
    return result_df

# if __name__ == "__main__":
#    main()

