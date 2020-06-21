import configparser
import psycopg2
import pandas as pd
from sql import copy_table_queries,insert_Dim_queries,insert_fact_queries,test_queries


"""
Load/Copy data from storage
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
Insert data into Dimention tables
"""
def insert_Dim(cur, conn):
    for query in insert_Dim_queries:
        cur.execute(query)
        conn.commit()

"""
Insert data into fact tables
"""
def insert_fact(cur, conn):
    for query in insert_fact_queries:
        cur.execute(query)
        conn.commit()

"""
Test data warehouse tables for data records
"""
def test_tables(conn):
    for query in test_queries:
        data = pd.read_sql(query,conn)
        print(data)

        
"""
main function to establish connection and call ETL functions to execute
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading staging tables")
    load_staging_tables(cur, conn)
    
    print("Loading Dimention tables")
    insert_Dim(cur, conn)
    
    print("Loading Fact tables")
    insert_fact(cur, conn)

    print("Test data tables")
    test_tables(conn)

    conn.close()


if __name__ == "__main__":
    main()