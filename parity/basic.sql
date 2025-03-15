SELECT schema_name
FROM information_schema.schemata
WHERE catalog_name = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_catalog = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, view_definition, is_secure, is_materialized
FROM information_schema.views
WHERE table_catalog = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, view_definition
FROM information_schema.views
WHERE table_catalog = 'PROD_DATABASE_NAME'
  AND is_materialized = 'YES';

