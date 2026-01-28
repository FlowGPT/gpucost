import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor  # For returning query results in dictionary format
from dataclasses import dataclass
from typing import List
import logging
import mysql.connector
from mysql.connector import Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


MYSQL_CONFIG = {
    'host': '18.189.20.30',    
    'user': 'flowgptwq',       
    'password': 'K7lfhE1qask1', 
    'database': 'flow_report_app',    
    'port': 9030,             
    'autocommit': False       
}

def get_mysql_connection():
    """Create and return a MySQL database connection"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("MySQL connection established successfully")
        return conn
    except Error as e:
        print(f"Connection failed: {e}")
        return None

@dataclass
class TokenCostResult:
    """Data class to encapsulate matched joined results"""
    id: str
    input_tokens: int
    output_tokens: int
    event_date: str

def get_matched_records(event_date: str):
    """
    Perform left join and return only matched records.
    Logs errors for unmatched records from ProviderTokenCost.
    """
    query = f"""
        SELECT 
            ptc.id, 
            tclmr.input_tokens, 
            tclmr.output_tokens,
            ptc.model,
            ptc.url,
            tclmr.event_date
        FROM 
            flow_rds_prod.view_ai_prod_provider_token_cost ptc
        LEFT JOIN 
            flow_report_app.tbl_chat_llm_model_request tclmr
            ON ptc.model = tclmr.model_id 
            AND ptc.url = tclmr.request_url
        where ptc.active = true and tclmr.event_date='{event_date}';
    """
    
    matched_results = []
    err_ids = []
    conn = get_mysql_connection()

    if conn is None:
        raise ValueError("Failed to establish database connection")

    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(query)

        for row in cur.fetchall():
            # Check if there was a match in the right table
            if row['input_tokens'] is None or row['output_tokens'] is None:
                # Log unmatched record
                logger.error(
                    f"No matching record found in tbl_chat_llm_model_request for "
                    f"ProviderTokenCost ID: {row['id']}, Model: {row['model']}, URL: {row['url']}"
                )
                err_ids.append(row['id'])
            else:
                # Add matched record to results
                matched_results.append(TokenCostResult(
                    id=row['id'],
                    input_tokens=row['input_tokens'],
                    output_tokens=row['output_tokens'],
                    event_date=row['event_date']
                ))
        
        return matched_results, err_ids
        
    except mysql.connector.Error as e:  # 修改为MySQL的错误处理
        logger.error(f"Database query error: {str(e)}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:  # 添加连接关闭
            conn.close()

@dataclass
class GPUHourCost:
    """Data class representing GPU hourly cost records, corresponding to database table structure"""
    model: str
    cluster: str
    card_num: int
    price: float

# Database connection parameters (please replace with your actual configuration)
DB_CONFIG_STAGE = {
    "host": "prod-1.cluster-cmg2ypxvbvye.us-east-2.rds.amazonaws.com",   # Database host address
    "port": "5432",            # Port, default is 5432
    "database": "ai_prod",      # Database name
    "user": "ai_prod",        # Username
    "password": "M9bV4kJ0pQ6wR2xT1dF8lH3gC5sZ7nY8" # Password
}
def get_pgdb_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG_STAGE["host"],
            port=DB_CONFIG_STAGE["port"],
            database=DB_CONFIG_STAGE["database"],
            user=DB_CONFIG_STAGE["user"],
            password=DB_CONFIG_STAGE["password"]
        )
        # Enable autocommit (optional, to avoid manual commit for each operation)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

def get_all_table_names():
    """Retrieve and print all table names in the current database"""
    conn = None
    try:
        conn = get_pgdb_connection()
        with conn.cursor() as cur:
            # Query to get all table names in the public schema
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            
            tables = cur.fetchall()
            print("\nAll tables in the database:")
            if tables:
                for table in tables:
                    print(f"- {table[0]}")
                return [table[0] for table in tables]
            else:
                print("No tables found in the database.")
                return []
    except Exception as e:
        print(f"Failed to retrieve table names: {e}")
        return []
    finally:
        if conn:
            conn.close()
def create_table():
    """Create example table (users table)"""
    conn = None
    try:
        conn = get_pgdb_connection()
        with conn.cursor() as cur:
            # Use sql.Identifier to prevent SQL injection (when dynamically generating table/column names)
            table_name = sql.Identifier("users")
            # Create table SQL
            create_sql = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INT,
                    email VARCHAR(100) UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(table_name)
            cur.execute(create_sql)
            print("Table created successfully (if it didn't exist)")
    except Exception as e:
        print(f"Failed to create table: {e}")
    finally:
        if conn:
            conn.close()

def insert_gpu_table(model, cluster, card_num, price):
    """Insert data into GPUHourCost table"""
    conn = None
    try:
        conn = get_pgdb_connection()
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO public."GPUHourCost" (model, cluster, "cardNum", price)
                VALUES (%s, %s, %s, %s)
                RETURNING model, "cardNum";
            """
            cur.execute(insert_sql, (model, cluster, card_num, price))
            print(f"Insert successful: Model={model}, Card Number={card_num}")
    except psycopg2.IntegrityError:
        print(f"Insert failed: Model={model}, Card Number={card_num} already exists (unique constraint)")
    except Exception as e:
        print(f"Failed to insert data: {e}")
    finally:
        if conn:
            conn.close()

def query_gpu_table(model=None, min_price=None, max_price=None):
    """Query data with optional filters"""
    conn = None
    try:
        conn = get_pgdb_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query_conditions = []
            params = []
            
            if model:
                query_conditions.append("model = %s")
                params.append(model)
            if min_price is not None:
                query_conditions.append("price >= %s")
                params.append(min_price)
            if max_price is not None:
                query_conditions.append("price <= %s")
                params.append(max_price)
            
            base_sql = "SELECT * FROM card_prices"
            if query_conditions:
                query_sql = f"{base_sql} WHERE {' AND '.join(query_conditions)} ORDER BY model, cardNum;"
            else:
                query_sql = f"{base_sql} ORDER BY model, cardNum;"
            
            cur.execute(query_sql, params)
            results = cur.fetchall()
            
            if model:
                print(f"Query results for model: {model}")
            elif min_price or max_price:
                price_range = []
                if min_price is not None:
                    price_range.append(f"≥ {min_price}")
                if max_price is not None:
                    price_range.append(f"≤ {max_price}")
                print(f"Query results with price {', '.join(price_range)}:")
            else:
                print("All records in card_prices:")
            
            for row in results:
                print(row)
            return results
    except Exception as e:
        print(f"Failed to query data: {e}")
    finally:
        if conn:
            conn.close()


def batch_insert_gpu_table(data_list, table_name):
    """Batch insert multiple records"""
    conn = None
    try:
        conn = get_pgdb_connection()
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO {table_name} (model, cluster, "cardNum", "price")
                VALUES (%s, %s, %s, %s);
            """
            cur.executemany(insert_sql, data_list)
            print(f"Batch insertion successful, inserted {len(data_list)} records")
    except Exception as e:
        print(f"Failed to batch insert: {e}")
    finally:
        if conn:
            conn.close()

def update_providercost_table(conn,id,inputmilcost,outputmilcost):
    """Update a record in the providercost table"""
    try:
        with conn.cursor() as cur:
            update_sql = """
                UPDATE public."ProviderTokenCost" 
                SET "inputCostMil" = %s, "outputCostMil" = %s
                WHERE id = %s;
            """
            cur.execute(update_sql, (inputmilcost, outputmilcost, id))
            conn.commit()
            print(f"Update successful for id: {id}")
    except Exception as e:
        print(f"Failed to update record: {e}")

def batch_insert_providercost_table(data_list, table_name):
    """Batch insert multiple records"""
    conn = None
    try:
        conn = get_pgdb_connection()
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO {table_name} (id, url, model, "inputCostMil", "outputCostMil", active)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            cur.executemany(insert_sql, data_list)
            print(f"Batch insertion successful, inserted {len(data_list)} records")
    except Exception as e:
        print(f"Failed to batch insert: {e}")
    finally:
        if conn:
            conn.close()

def get_by_cluster(conn, cluster: str) -> List[GPUHourCost]:
    """
    Query all GPU hourly cost records for a specified cluster
    
    :param cluster: Name of the cluster to query
    :return: List of GPUHourCost objects containing the query results
    """
    query = """
        SELECT model, cluster, "cardNum" as card_num, price 
        FROM "GPUHourCost" 
        WHERE cluster = %s
    """

    result = []
    cursor = None
    
    try:
        # Use the provided connection
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (cluster,))
        
        # Convert query results to list of GPUHourCost objects
        for row in cursor.fetchall():
            result.append(GPUHourCost(
                model=row['model'],
                cluster=row['cluster'],
                card_num=row['card_num'],
                price=row['price']
            ))
            
        return result
        
    except psycopg2.Error as e:
        print(f"Database query error: {e}")
        conn.rollback()
        return []
        
    finally:
        if cursor:
            cursor.close()



