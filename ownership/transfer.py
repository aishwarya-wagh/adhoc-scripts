import pandas as pd
import os
import re

# Configuration - Update these paths to match your files
PROD_OWNERSHIP_CSV = 'prod_table_ownership.csv'  # schema,table,owner
TARGET_STATE_CSV = 'target_table_state.csv'      # schema,table,current_owner
VALID_ROLES_CSV = 'valid_roles.csv'             # role_name
OUTPUT_DIR = 'ownership_transfers'

def clean_name(name):
    """Sanitize names for safe filename use"""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def load_data():
    """Load and validate input CSV files"""
    try:
        # Load production ownership data
        prod_df = pd.read_csv(PROD_OWNERSHIP_CSV)
        if not {'schema', 'table', 'owner'}.issubset(prod_df.columns):
            raise ValueError("Production CSV must contain: schema, table, owner")
        
        # Load target environment data
        target_df = pd.read_csv(TARGET_STATE_CSV)
        if not {'schema', 'table', 'current_owner'}.issubset(target_df.columns):
            raise ValueError("Target CSV must contain: schema, table, current_owner")
        
        # Load valid roles
        valid_roles = pd.read_csv(VALID_ROLES_CSV)['role_name'].tolist()
        
        return prod_df, target_df, valid_roles
    
    except FileNotFoundError as e:
        print(f"Error: Missing input file - {str(e)}")
        return None, None, None
    except pd.errors.EmptyDataError:
        print("Error: One or more CSV files are empty")
        return None, None, None

def generate_transfer_statements():
    """Generate ownership transfer SQL files"""
    # Load and validate data
    prod_df, target_df, valid_roles = load_data()
    if prod_df is None:
        return
    
    # Create output directories
    os.makedirs(os.path.join(OUTPUT_DIR, 'bulk'), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, 'individual'), exist_ok=True)
    
    # Merge production and target data
    merged_df = pd.merge(
        prod_df,
        target_df,
        on=['schema', 'table'],
        how='inner',  # Only tables that exist in both
        suffixes=('_prod', '_target')
    )
    
    # Filter out tables where owner hasn't changed
    changed_ownership = merged_df[merged_df['owner_prod'] != merged_df['owner_target']]
    
    # Group by schema to find uniform ownership
    schema_groups = changed_ownership.groupby('schema')['owner_prod'].agg(['nunique', 'first'])
    uniform_schemas = schema_groups[schema_groups['nunique'] == 1]
    
    # Generate bulk transfers for uniform schemas
    for schema, row in uniform_schemas.iterrows():
        owner = row['first']
        if owner not in valid_roles:
            print(f"Skipping schema {schema} - invalid role: {owner}")
            continue
            
        # Count tables in this schema
        table_count = len(changed_ownership[changed_ownership['schema'] == schema])
        
        # Write bulk transfer file
        filename = f"bulk_{clean_name(schema)}.sql"
        with open(os.path.join(OUTPUT_DIR, 'bulk', filename), 'w') as f:
            f.write(f"-- TRANSFER ALL TABLES IN SCHEMA {schema}\n")
            f.write(f"-- Production owner: {owner}\n")
            f.write(f"-- Affects {table_count} tables\n\n")
            f.write(f"GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {schema} ")
            f.write(f"TO ROLE {owner} COPY CURRENT GRANTS;\n")
    
    # Generate individual transfers for remaining tables
    non_uniform = changed_ownership[~changed_ownership['schema'].isin(uniform_schemas.index)]
    
    for _, row in non_uniform.iterrows():
        if row['owner_prod'] not in valid_roles:
            print(f"Skipping {row['schema']}.{row['table']} - invalid role: {row['owner_prod']}")
            continue
            
        # Create individual transfer file
        filename = f"indiv_{clean_name(row['schema'])}_{clean_name(row['table'])}.sql"
        with open(os.path.join(OUTPUT_DIR, 'individual', filename), 'w') as f:
            f.write(f"-- TRANSFER {row['schema']}.{row['table']}\n")
            f.write(f"-- Production owner: {row['owner_prod']}\n")
            f.write(f"-- Current owner: {row['owner_target']}\n\n")
            f.write(f"GRANT OWNERSHIP ON TABLE {row['schema']}.{row['table']} ")
            f.write(f"TO ROLE {row['owner_prod']} COPY CURRENT GRANTS;\n")
    
    print(f"Generated {len(uniform_schemas)} bulk and {len(non_uniform)} individual transfer files")

if __name__ == "__main__":
    print("Starting ownership transfer script...")
    generate_transfer_statements()
    print("Script completed. Check the output directory for generated files.")
