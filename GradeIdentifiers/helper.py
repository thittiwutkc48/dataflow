import psycopg2
import apache_beam as beam
from dotenv import load_dotenv
import os

load_dotenv()

connection_properties = {
    'instance': os.getenv('DB_INSTANCE'),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'jdbc_url': os.getenv('DB_JDBC_URL'),
    'port': int(os.getenv('DB_PORT')),
    'host': os.getenv('DB_HOST'),
}


schema_name="grade"
table_name = "grade_identifiers"


def create_table():
    create_table_query = f"""
    DROP TABLE IF EXISTS {schema_name}.{table_name}_temp;

    CREATE TABLE {schema_name}.{table_name}_temp (
            id bigserial NOT NULL,
            acc_uuid text NULL,
            cus_uuid text NULL,
            citizen_iden_uuid text NULL,
            tm_key_mth varchar(50) NULL,
            identifier_id varchar(100) NULL,
            identifier_name varchar(100) NULL,
            identifier_desc varchar(100) NULL,
            identifier_status varchar(1) NULL,
            bill_status varchar(1) NULL,
            identifier_start_date timestamptz NULL,
            identifier_aging int4 NULL,
            total_amount float8 NULL,
            avg_amount float8 NULL,
            cur_inv_amount float8 NULL,
            operator_name varchar(50) NULL,
            ban_number varchar(50) NULL,
            golden_id int4 NULL,
            audit_cu int4 NULL,
            audit_cd timestamptz NULL,
            audit_mu int4 NULL,
            audit_md timestamptz NULL
    );
    """
    
    try:
        connection_string = f"dbname='{connection_properties['database']}' user='{connection_properties['user']}' password='{connection_properties['password']}' host='{connection_properties['host']}' port='{connection_properties['port']}'"
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()  
                print(f"Table {schema_name}.{table_name}_temp created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")


def read_last_id_from_postgres():
    query = f"SELECT MAX(id) FROM {schema_name}.{table_name}"
    last_id = 0
    connection_string = f"dbname='{connection_properties['database']}' user='{connection_properties['user']}' password='{connection_properties['password']}' host='{connection_properties['host']}' port='{connection_properties['port']}'"
    
    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result and result[0] is not None:
                    last_id = result[0]
    except psycopg2.Error as e:
        print(f"Error executing last_id query: {e}")
    
    return last_id + 1


class AddIncrementalId(beam.DoFn):
    def __init__(self, start_id):
        self.start_id = start_id

    def process(self, element):
        element['id'] = self.start_id
        self.start_id += 1 
        yield element


def partition_fn(element, num_partitions):
    return element['id'] % num_partitions


def execute_sql_query():
    insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (
            id,
            acc_uuid,
            cus_uuid,
            citizen_iden_uuid,
            tm_key_mth,
            identifier_id,
            identifier_name,
            identifier_desc,
            identifier_status,
            bill_status,
            identifier_start_date,
            identifier_aging,
            total_amount,
            avg_amount,
            cur_inv_amount,
            operator_name,
            ban_number,
            golden_id,
            audit_cu,
            audit_cd,
            audit_mu,
            audit_md
        )
        SELECT
            id,
            acc_uuid::uuid,
            cus_uuid::uuid,
            citizen_iden_uuid::uuid,
            tm_key_mth,
            identifier_id,
            identifier_name,
            identifier_desc,
            identifier_status,
            bill_status,
            identifier_start_date,
            identifier_aging,
            total_amount,
            avg_amount,
            cur_inv_amount,
            operator_name,
            ban_number,
            golden_id,
            audit_cu,
            audit_cd,
            audit_mu,
            audit_md
        FROM {schema_name}.{table_name}_temp;


    DROP TABLE IF EXISTS {schema_name}.{table_name}_temp;

    """

    connection_string = f"dbname='{connection_properties['database']}' user='{connection_properties['user']}' password='{connection_properties['password']}' host='{connection_properties['host']}' port='{connection_properties['port']}'"
    
    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                conn.commit()
                print(f"Query Insert to {schema_name}.{table_name} executed successfully.")
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")
