import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_sequence_queries, drop_sequence_queries


def create_database():
    # connect to default database
    print('Inside function create_database()')
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database - studentdb")
        print(e)
    
    try:
        conn.set_session(autocommit=True)
    except psycopg2.Error as e:
        print("Error: Could not set auto commit")
        print(e)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not open the cursor")
        print(e)

    
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error: Could DROP the database sparkifydb")
        print(e)
        
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Could not CREATE the database sparkifydb")
        print(e)

    # close connection to default database
    try:
        conn.close()    
    except psycopg2.Error as e:
        print("Error: Could close the connection to database sparkifydb")
        print(e)
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not connect to the database sparkifydb")
        print(e)
        
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not open cursor in database sparkifydb")
        print(e)
    
    return cur, conn


def drop_tables(cur, conn):
    print('Inside function drop_tables()')
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not execute queries in drop_table_queries")
            print(e)
        conn.commit()

def drop_sequence(cur, conn):
    print('Inside function drop_sequence()')
    for query in drop_sequence_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not execute queries in drop_sequence_queries")
            print(e)
        conn.commit()


def create_tables(cur, conn):
    print('Inside function create_tables()')
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not execute queries in create_table_queries")
            print(e)
        conn.commit()
        
def create_sequence(cur, conn):
    print('Inside function create_sequence()')
    for query in create_sequence_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not execute queries in create_sequence_queries")
            print(e)
        conn.commit()


def main():
    print('Inside main()')
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    drop_sequence(cur, conn)
    create_tables(cur, conn)
    create_sequence(cur, conn)

    conn.close()
    
    print('\nTables/Sequences created successfully.')


if __name__ == "__main__":
    main()
