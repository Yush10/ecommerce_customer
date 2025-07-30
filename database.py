import duckdb
import pandas as pd
import os
import toml
import csv

# CSV and table mappings
csv_table_mapping = {
    "Cust_Behavior_Final.csv": {
        "table_name": "customers",
        "schema": """
            CREATE TABLE IF NOT EXISTS customers (
                Time_on_site FLOAT,
                Pages_viewed INTEGER,
                Clicked_ad BOOLEAN,
                Cart_value FLOAT,
                Referral VARCHAR,
                Browser_Refresh_Rate FLOAT,
                Last_Ad_Seen VARCHAR,
                Purchase BOOLEAN,
                ID INTEGER,
                Date_Accessed DATE,
                Platform_Num INTEGER
            );
        """
    },
    "cpc_table_updated.csv": {
        "table_name": "cpc",
        "schema": """
            CREATE TABLE IF NOT EXISTS cpc (
                Plat_Num INTEGER PRIMARY KEY,
                Platform VARCHAR,
                Average_CPC FLOAT,
            );
        """
    }
}

def create_database(database_path):
    """
    Create a DuckDB database file
    
    Args:
        database_path (str): Path where the database file will be created
    """
    try:
        # Check if database already exists
        if os.path.exists(database_path):
            print(f"Database '{database_path}' already exists.")
        else:
            # Create connection (this automatically creates the database file)
            conn = duckdb.connect(database_path)
            conn.close()
            print(f"Database '{database_path}' created successfully.")
            
    except Exception as e:
        print(f"Error creating database: {e}")

def connect_to_db(database_path):
    """Connect to DuckDB database"""
    return duckdb.connect(database_path)

def create_tables(conn):
    """Create tables in the database"""
    conn.execute("DROP TABLE IF EXISTS cpc")
    conn.execute("DROP TABLE IF EXISTS ecommerce_database.customers")

    for csv_file, table_info in csv_table_mapping.items():
        conn.execute(table_info["schema"])
        print(f"Table '{table_info['table_name']}' created or already exists.")


def load_csv_to_table(conn):
    """Load CSV data into tables"""
    # Step 1: Load everything as strings
    conn.execute("""
    CREATE TEMP TABLE temp_data AS
    SELECT * FROM read_csv_auto('Cust_Behavior_Final.csv', 
        header=true,
        delim=',',
        all_varchar=true
    )
    """)

    conn.execute("""
    CREATE TEMP TABLE temp_data2 AS
    SELECT * FROM read_csv_auto('cpc_table_updated.csv',
        header=true,
        delim=',',
        all_varchar=true
    )
    """)        

    
    # Step 2: Insert with proper conversions
    conn.execute("""
    INSERT INTO customers 
    SELECT 
        CAST(Time_on_site AS FLOAT),
        CAST(Pages_viewed AS INTEGER),
        CASE WHEN Clicked_ad = '1' THEN true ELSE false END,
        CAST(Cart_value AS FLOAT),
        Referral,
        CAST(Browser_Refresh_Rate AS FLOAT),
        Last_Ad_Seen,
        CASE WHEN Purchase = '1' THEN true ELSE false END,
        CAST(ID AS INTEGER),
        CAST(Date_Accessed AS DATE),
        CAST(Platform_Num AS INTEGER)
    FROM temp_data
    """)

    conn.execute("""
    INSERT INTO cpc
    SELECT 
        CAST(Plat_Num AS INTEGER),
        Platform,
        CAST(Average_CPC AS FLOAT),

    FROM temp_data2
    """)
    
    # Step 3: Clean up
    conn.execute("DROP TABLE temp_data")
    conn.execute("DROP TABLE temp_data2")

'''

    for csv_file, table_info in csv_table_mapping.items():
        if not os.path.exists(csv_file):
            print(f"Warning: CSV file '{csv_file}' not found. Skipping.")
            continue
            
        try:
            df = pd.read_csv(csv_file)
            table_name = table_info["table_name"]
            
            # Use DuckDB's efficient INSERT from DataFrame
            conn.register('temp_df', df)
            
            # Get column names from DataFrame
            columns = ', '.join(df.columns)
            
            # Insert data using DuckDB's DataFrame integration
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
            
            print(f"Loaded {len(df)} rows from '{csv_file}' into table '{table_name}'.")
            
        except Exception as e:
            print(f"Error loading '{csv_file}': {e}")
'''
                 
def load_csv_direct(conn):
    """Alternative method: Load CSV files directly using DuckDB's native CSV reader"""
    for csv_file, table_info in csv_table_mapping.items():
        if not os.path.exists(csv_file):
            print(f"Warning: CSV file '{csv_file}' not found. Skipping.")
            continue
            
        table_name = table_info["table_name"]
        
        try:
            # DuckDB can read CSV directly - much faster for large files
            conn.execute(f"""
                INSERT INTO {table_name} 
                SELECT * FROM read_csv_auto('{csv_file}')
            """)
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"Loaded {row_count} rows from '{csv_file}' into table '{table_name}' using direct CSV read.")
            
        except Exception as e:
            print(f"Error loading '{csv_file}' directly: {e}")

def get_database_connection(database_path):
    """Get database connection to DuckDB"""
    try:
        conn = duckdb.connect(database_path)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


if __name__ == "__main__":

    # For DuckDB, we just need the database file path
    database_path = "ecommerce_database.duckdb"
    

    print(f"Using database: {database_path}")
    
    # Create database
    create_database(database_path)
    
    # Connect and set up tables
    conn = connect_to_db(database_path)
    
    try:
        # Create tables
        create_tables(conn)
        
        # Load data from CSV files
        # Method 1: Using pandas (good for data transformation)
        load_csv_to_table(conn)
        
        # Method 2: Direct CSV loading (uncomment if you prefer this - faster for large files)
        # load_csv_direct(conn)
        
        print("Database setup complete.")
        
        # Example: Query the data to verify
        print("\nVerifying data load:")
        for csv_file, table_info in csv_table_mapping.items():
            table_name = table_info["table_name"]
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"Table '{table_name}': {count} rows")
            except Exception as e:
                print(f"Error querying '{table_name}': {e}")
                
    finally:
        conn.close()