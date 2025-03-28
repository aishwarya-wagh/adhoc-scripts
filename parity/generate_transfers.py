import pandas as pd
import os
import snowflake.connector
from snowflake.connector import ProgrammingError

# Configuration
INPUT_CSV = 'table_ownership.csv'  # Expected columns: schema_name, table_name, owner
OUTPUT_DIR = 'ownership_transfers'
BULK_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'bulk_schema_transfers')
INDIVIDUAL_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'individual_transfers')

# Snowflake connection for validation (optional)
VALIDATE = True  # Set to False to skip validation
SF_ACCOUNT = 'your_account'
SF_USER = 'your_user'
SF_PASSWORD = 'your_password'
SF_WAREHOUSE = 'your_warehouse'
SF_ROLE = 'ACCOUNTADMIN'

def validate_connection():
    """Test Snowflake connection and get valid roles/tables"""
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE
    )
    
    # Get all existing roles
    with conn.cursor() as cur:
        cur.execute("SHOW ROLES")
        valid_roles = [row[1] for row in cur.fetchall()]
        
        # Get all existing tables
        cur.execute("""
        SELECT table_schema || '.' || table_name 
        FROM information_schema.tables 
        WHERE table_type = 'BASE TABLE'
        """)
        valid_tables = set(row[0] for row in cur.fetchall())
    
    conn.close()
    return valid_roles, valid_tables

def generate_transfer_statements(df, valid_roles, valid_tables):
    """Generate transfer statements with validation"""
    # Create output directories
    os.makedirs(BULK_OUTPUT_DIR, exist_ok=True)
    os.makedirs(INDIVIDUAL_OUTPUT_DIR, exist_ok=True)
    
    # Track schemas with uniform ownership
    schema_owners = df.groupby('schema_name')['owner'].agg(['unique', 'count'])
    uniform_schemas = schema_owners[schema_owners['unique'].str.len() == 1]
    
    # Generate bulk schema transfers
    for schema, row in uniform_schemas.iterrows():
        owner = row['unique'][0]
        
        if VALIDATE:
            if owner not in valid_roles:
                print(f"Skipping schema {schema} - invalid role: {owner}")
                continue
        
        filename = f"bulk_transfer_{schema}.sql"
        with open(os.path.join(BULK_OUTPUT_DIR, filename), 'w') as f:
            f.write(f"-- Bulk ownership transfer for schema {schema}\n")
            f.write(f"-- All tables owned by: {owner}\n\n")
            f.write(f"GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {schema} TO ROLE {owner} COPY CURRENT GRANTS;\n")
    
    # Generate individual transfers for non-uniform schemas
    non_uniform = df[~df['schema_name'].isin(uniform_schemas.index)]
    
    for _, row in non_uniform.iterrows():
        full_table_name = f"{row['schema_name']}.{row['table_name']}"
        
        if VALIDATE:
            if row['owner'] not in valid_roles:
                print(f"Skipping {full_table_name} - invalid role: {row['owner']}")
                continue
            if full_table_name not in valid_tables:
                print(f"Skipping {full_table_name} - table not found")
                continue
        
        filename = f"transfer_{row['schema_name']}_{row['table_name']}.sql"
        with open(os.path.join(INDIVIDUAL_OUTPUT_DIR, filename), 'w') as f:
            f.write(f"-- Ownership transfer for {full_table_name}\n")
            f.write(f"-- New owner: {row['owner']}\n\n")
            f.write(f"GRANT OWNERSHIP ON TABLE {full_table_name} TO ROLE {row['owner']} COPY CURRENT GRANTS;\n")

def main():
    # Load ownership data
    df = pd.read_csv(INPUT_CSV)
    
    # Validate against Snowflake if enabled
    valid_roles, valid_tables = [], set()
    if VALIDATE:
        try:
            print("Validating roles and tables against Snowflake...")
            valid_roles, valid_tables = validate_connection()
            print(f"Found {len(valid_roles)} valid roles and {len(valid_tables)} tables")
        except Exception as e:
            print(f"Validation failed: {str(e)}")
            return
    
    # Generate transfer statements
    generate_transfer_statements(df, valid_roles, valid_tables)
    print("Transfer statements generated successfully")

if __name__ == "__main__":
    main()
