import pandas as pd
import os

# Configuration
PROD_CSV = 'prod_ownership.csv'  # From production (schema, table, owner)
TARGET_CSV = 'target_tables.csv'  # From target account (schema, table, current_owner)
VALID_ROLES_CSV = 'valid_roles.csv'  # List of valid roles in target
OUTPUT_DIR = 'ownership_transfers'

def load_and_validate_data():
    """Load and validate all CSV data"""
    # Load production ownership data
    prod_df = pd.read_csv(PROD_CSV)
    required_cols = {'schema_name', 'table_name', 'owner'}
    if not required_cols.issubset(prod_df.columns):
        raise ValueError(f"Production CSV missing required columns: {required_cols}")
    
    # Load target environment data
    target_df = pd.read_csv(TARGET_CSV)
    target_cols = {'schema_name', 'table_name', 'current_owner'}
    if not target_cols.issubset(target_df.columns):
        raise ValueError(f"Target CSV missing required columns: {target_cols}")
    
    # Load valid roles
    valid_roles = pd.read_csv(VALID_ROLES_CSV)['role_name'].tolist()
    
    return prod_df, target_df, valid_roles

def generate_transfers(prod_df, target_df, valid_roles):
    """Generate transfer statements from CSV data"""
    # Merge production and target data
    merged_df = pd.merge(
        prod_df,
        target_df,
        on=['schema_name', 'table_name'],
        how='inner'  # Only tables that exist in both
    )
    
    # Create output directories
    bulk_dir = os.path.join(OUTPUT_DIR, 'bulk_schema_transfers')
    indiv_dir = os.path.join(OUTPUT_DIR, 'individual_transfers')
    os.makedirs(bulk_dir, exist_ok=True)
    os.makedirs(indiv_dir, exist_ok=True)
    
    # Track processed schemas
    processed_schemas = set()
    
    # First pass - identify schemas with uniform ownership
    schema_stats = merged_df.groupby('schema_name')['owner'].agg(['nunique', 'first'])
    uniform_schemas = schema_stats[schema_stats['nunique'] == 1]
    
    # Generate bulk transfers for uniform schemas
    for schema, row in uniform_schemas.iterrows():
        owner = row['first']
        if owner not in valid_roles:
            print(f"Skipping schema {schema} - invalid role: {owner}")
            continue
            
        filename = f"bulk_{schema}.sql"
        with open(os.path.join(bulk_dir, filename), 'w') as f:
            f.write(f"GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {schema} ")
            f.write(f"TO ROLE {owner} COPY CURRENT GRANTS;\n")
            f.write(f"-- Production owner: {owner}\n")
            f.write(f"-- Affects {len(merged_df[merged_df['schema_name'] == schema])} tables\n")
        
        processed_schemas.add(schema)
    
    # Second pass - individual tables in non-uniform schemas
    non_uniform = merged_df[~merged_df['schema_name'].isin(processed_schemas)]
    
    for _, row in non_uniform.iterrows():
        if row['owner'] not in valid_roles:
            print(f"Skipping {row['schema_name']}.{row['table_name']} - invalid role: {row['owner']}")
            continue
            
        if row['owner'] == row['current_owner']:
            continue  # No ownership change needed
            
        # Create safe filename
        safe_name = f"{row['schema_name']}_{row['table_name']}".replace('.', '_')
        filename = f"indiv_{safe_name}.sql"
        
        with open(os.path.join(indiv_dir, filename), 'w') as f:
            f.write(f"GRANT OWNERSHIP ON TABLE {row['schema_name']}.{row['table_name']} ")
            f.write(f"TO ROLE {row['owner']} COPY CURRENT GRANTS;\n")
            f.write(f"-- Production owner: {row['owner']}\n")
            f.write(f"-- Current owner: {row['current_owner']}\n")
    
    print(f"Generated {len(uniform_schemas)} bulk and {len(non_uniform)} individual transfer files")

def main():
    try:
        print("Loading and validating CSV data...")
        prod_df, target_df, valid_roles = load_and_validate_data()
        
        print("Generating transfer statements...")
        generate_transfers(prod_df, target_df, valid_roles)
        
        print(f"Transfer files created in '{OUTPUT_DIR}' directory")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
