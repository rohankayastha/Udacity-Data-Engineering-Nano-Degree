import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This function Drops the tables if they already exist.
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        """  
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not execute queries in drop_table_queries")
            print(e)
        conn.commit()


def create_tables(cur, conn):
    """
        This function creates the staging and Fact/Dimension tables.
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        """  
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not execute queries in create_table_queries")
            print(e)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Redshift database")
        print(e)
        
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not open the cursor")
        print(e)

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()