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

# Generate DDL for missing schemas
missing_schemas = set(prod_schemas['schema_name']) - set(qa_schemas['schema_name'])
with open('create_schemas_qa.sql', 'w') as f:
    for schema in missing_schemas:
        f.write(f"CREATE SCHEMA {schema};\n")

# Generate ALTER TABLE statements for missing columns
merged_columns = pd.merge(prod_columns, qa_columns, on=['table_schema', 'table_name', 'column_name'], how='outer', indicator=True)
missing_columns = merged_columns[merged_columns['_merge'] == 'left_only']

with open('alter_tables_qa.sql', 'w') as f:
    for _, row in missing_columns.iterrows():
        f.write(
            f"ALTER TABLE {row['table_schema']}.{row['table_name']} "
            f"ADD COLUMN {row['column_name']} {row['data_type']} "
            f"{'NOT NULL' if row['is_nullable'] == 'NO' else ''};\n"
        )

# Generate CREATE OR REPLACE VIEW statements for views
merged_views = pd.merge(prod_views, qa_views, on=['table_schema', 'table_name'], how='outer', indicator=True)
missing_views = merged_views[merged_views['_merge'] == 'left_only']

with open('update_views_qa.sql', 'w') as f:
    for _, row in missing_views.iterrows():
        f.write(
            f"CREATE OR REPLACE VIEW {row['table_schema']}.{row['table_name']} AS {row['view_definition']};\n"
        )
