import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function loads the Staging tables. This invokes the COPY utility. The data to be loaded is stored as JSON files in a S3 bucket.
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        """    
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        This function inserts data into the Fact and Dimension tables. Source of the data are the staging tables.
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        """ 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()