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
    missing_schemas = set(prod_schemas['schema_name']) - set(qa_schemas['schema_name'])
    for schema in missing_schemas:
        schema_file.write(f"CREATE SCHEMA IF NOT EXISTS {schema};\n")

    # Step 2: Create missing tables
    merged_tables = pd.merge(prod_tables, qa_tables, on=['table_schema', 'table_name'], how='outer', indicator=True)
    missing_tables = merged_tables[merged_tables['_merge'] == 'left_only']

    for _, row in missing_tables.iterrows():
        # Generate CREATE TABLE statement
        table_columns = prod_columns[
            (prod_columns['table_schema'] == row['table_schema']) &
            (prod_columns['table_name'] == row['table_name'])
        ]
        columns_ddl = []
        for _, col in table_columns.iterrows():
            col_def = f"{col['column_name']} {col['data_type']}"
            if col['is_nullable'] == 'NO':
                col_def += " NOT NULL"
            if pd.notna(col['column_default']):
                col_def += f" DEFAULT {col['column_default']}"
            columns_ddl.append(col_def)
        
        create_table_ddl = f"CREATE TABLE IF NOT EXISTS {row['table_schema']}.{row['table_name']} (\n"
        create_table_ddl += ",\n".join(columns_ddl)
        create_table_ddl += "\n);"
        table_file.write(create_table_ddl + "\n\n")

    # Step 3: Create missing views
    merged_views = pd.merge(prod_views, qa_views, on=['table_schema', 'table_name'], how='outer', indicator=True)
    missing_views = merged_views[merged_views['_merge'] == 'left_only']

    for _, row in missing_views.iterrows():
        create_view_ddl = f"CREATE OR REPLACE VIEW {row['table_schema']}.{row['table_name']} AS {row['view_definition']};\n"
        view_file.write(create_view_ddl)

    # Step 4: Alter existing tables to add missing columns
    merged_columns = pd.merge(prod_columns, qa_columns, on=['table_schema', 'table_name', 'column_name'], how='outer', indicator=True)
    missing_columns = merged_columns[merged_columns['_merge'] == 'left_only']

    for _, row in missing_columns.iterrows():
        alter_table_ddl = f"ALTER TABLE {row['table_schema']}.{row['table_name']} "
        alter_table_ddl += f"ADD COLUMN {row['column_name']} {row['data_type']}"
        if row['is_nullable'] == 'NO':
            alter_table_ddl += " NOT NULL"
        if pd.notna(row['column_default']):
            alter_table_ddl += f" DEFAULT {row['column_default']}"
        alter_table_ddl += ";\n"
        alter_file.write(alter_table_ddl)

print("DDL scripts generated successfully!")
