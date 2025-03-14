import pandas as pd

# Load prod metadata
prod_schemas = pd.read_csv('schemas.csv')
prod_tables = pd.read_csv('tables.csv')
prod_columns = pd.read_csv('columns.csv')
prod_views = pd.read_csv('views.csv')

# Load QA metadata
qa_schemas = pd.read_csv('qa_schemas.csv')
qa_tables = pd.read_csv('qa_tables.csv')
qa_columns = pd.read_csv('qa_columns.csv')
qa_views = pd.read_csv('qa_views.csv')

# Output files
with open('create_schemas_qa.sql', 'w') as schema_file, \
     open('create_tables_qa.sql', 'w') as table_file, \
     open('create_views_qa.sql', 'w') as view_file, \
     open('alter_tables_qa.sql', 'w') as alter_file:

    # Step 1: Create missing schemas
    missing_schemas = set(prod_schemas['SCHEMA_NAME']) - set(qa_schemas['SCHEMA_NAME'])
    for schema in missing_schemas:
        schema_file.write(f"CREATE SCHEMA IF NOT EXISTS {schema};\n")

    # Step 2: Create missing tables
    merged_tables = pd.merge(prod_tables, qa_tables, on=['TABLE_SCHEMA', 'TABLE_NAME'], how='outer', indicator=True)
    missing_tables = merged_tables[merged_tables['_merge'] == 'left_only']

    for _, row in missing_tables.iterrows():
        # Generate CREATE TABLE statement
        table_columns = prod_columns[
            (prod_columns['TABLE_SCHEMA'] == row['TABLE_SCHEMA']) &
            (prod_columns['TABLE_NAME'] == row['TABLE_NAME'])
        ]
        columns_ddl = []
        for _, col in table_columns.iterrows():
            col_def = f"{col['COLUMN_NAME']} {col['DATA_TYPE']}"
            if col['IS_NULLABLE'] == 'NO':
                col_def += " NOT NULL"
            if pd.notna(col['COLUMN_DEFAULT']):
                col_def += f" DEFAULT {col['COLUMN_DEFAULT']}"
            columns_ddl.append(col_def)
        
        create_table_ddl = f"CREATE TABLE IF NOT EXISTS {row['TABLE_SCHEMA']}.{row['TABLE_NAME']} (\n"
        create_table_ddl += ",\n".join(columns_ddl)
        create_table_ddl += "\n);"
        table_file.write(create_table_ddl + "\n\n")

    # Step 3: Create missing views
    merged_views = pd.merge(prod_views, qa_views, on=['TABLE_SCHEMA', 'TABLE_NAME'], how='outer', indicator=True)
    missing_views = merged_views[merged_views['_merge'] == 'left_only']

    for _, row in missing_views.iterrows():
        create_view_ddl = f"CREATE OR REPLACE VIEW {row['TABLE_SCHEMA']}.{row['TABLE_NAME']} AS {row['VIEW_DEFINITION']};\n"
        view_file.write(create_view_ddl)

    # Step 4: Alter existing tables to add missing columns
    merged_columns = pd.merge(prod_columns, qa_columns, on=['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME'], how='outer', indicator=True)
    missing_columns = merged_columns[merged_columns['_merge'] == 'left_only']

    for _, row in missing_columns.iterrows():
        alter_table_ddl = f"ALTER TABLE {row['TABLE_SCHEMA']}.{row['TABLE_NAME']} "
        alter_table_ddl += f"ADD COLUMN {row['COLUMN_NAME']} {row['DATA_TYPE']}"
        if row['IS_NULLABLE'] == 'NO':
            alter_table_ddl += " NOT NULL"
        if pd.notna(row['COLUMN_DEFAULT']):
            alter_table_ddl += f" DEFAULT {row['COLUMN_DEFAULT']}"
        alter_table_ddl += ";\n"
        alter_file.write(alter_table_ddl)

print("DDL scripts generated successfully!")
