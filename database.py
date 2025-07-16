import psycopg2
import pandas as pd
import os
import toml



# CSV and table mappings
csv_table_mapping = {
    "ecommerce_cust_behavior.csv": {
        "table_name": "customers",
        "schema": """
            CREATE TABLE IF NOT EXISTS customers (
                Plat_Num PRIMARY KEY,
                Time_on_sight FLOAT,
                Pages_Viewed FLOAT,
                Clicked_Ad BINARY
                Cart_Value FLOAT
                Referral TEXT
                Browser_Refresh_Rate FLOAT
                Last_Ad_Seen TEXT
                purchase BINARY
                ID INTEGER
                Date_Accessed DATE
            );
        """
    },
    "cpc_table.csv": {
        "table_name": "cpc",
        "schema": """
            CREATE TABLE IF NOT EXISTS cpc (
                Platform_Number (?) INTEGER PRIMARY KEY,
                Platform TEXT,
                Average_CPC FLOAT,
                FOREIGN KEY (Platform_Num) REFERENCES customers(Platform_Num)
            );
        """
    }
}

def create_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE_NAME}';")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(f"CREATE DATABASE {DATABASE_NAME};")
        print(f"Database '{DATABASE_NAME}' created.")
    else:
        print(f"Database '{DATABASE_NAME}' already exists.")
    cursor.close()
    conn.close()

def connect_to_db():
    return psycopg2.connect(
        dbname=DATABASE_NAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

def create_tables(conn):
    cursor = conn.cursor()
    for csv_file, table_info in csv_table_mapping.items():
        cursor.execute(table_info["schema"])
        print(f"Table '{table_info['table_name']}' created or already exists.")
    conn.commit()
    cursor.close()

def load_csv_to_table(conn):
    cursor = conn.cursor()
    for csv_file, table_info in csv_table_mapping.items():
        df = pd.read_csv(csv_file)
        table_name = table_info["table_name"]
        for i, row in df.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(insert_query, tuple(row))
        print(f"Loaded data from '{csv_file}' into table '{table_name}'.")
    conn.commit()
    cursor.close()

def main():
    create_database()
    conn = connect_to_db()
    create_tables(conn)
    load_csv_to_table(conn)
    conn.close()
    print("Database setup complete.")

#if __name__ == "__main__":
#    main()

####Secrets 

def load_secrets():
    """Load secrets from secrets.toml file"""
    try:
        with open('secrets.toml', 'r') as f:
            secrets = toml.load(f)
        return secrets
    except FileNotFoundError:
        print("Error: secrets.toml file not found!")
        return None
    except Exception as e:
        print(f"Error loading secrets: {e}")
        return None

def get_database_connection():
    """Get database connection using credentials from secrets.toml"""
    secrets = load_secrets()
    if not secrets:
        return None
   
    db_config = secrets.get('database', {})
   
    # Example for PostgreSQL using psycopg2
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=db_config.get('host', 'localhost'),
            database=db_config.get('database_name'),
            user=db_config.get('username'),
            password=db_config.get('password'),
            port=db_config.get('port', 5432)
        )
        return conn
    except ImportError:
        print("psycopg2 not installed. Install with: pip install psycopg2-binary")
        return None
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Load secrets
    secrets = load_secrets()
    if secrets:
        print("Secrets loaded successfully!")
       
        # Get database connection
        conn = get_database_connection()
        if conn:
            print("Database connected successfully!")
            # Your database operations here
            conn.close()
        else:
            print("Failed to connect to database")
    else:
        print("Failed to load secrets")