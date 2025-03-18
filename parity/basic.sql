SELECT schema_name
FROM information_schema.schemata
WHERE catalog_name = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_catalog = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, view_definition, is_secure
FROM information_schema.views
WHERE table_catalog = 'PROD_DATABASE_NAME';

SELECT table_schema, table_name, 'MATERIALIZED VIEW' AS table_type
FROM information_schema.tables
WHERE table_catalog = 'PROD_DATABASE_NAME'
  AND table_type = 'MATERIALIZED VIEW';

SHOW SEQUENCES IN DATABASE YOUR_DATABASE_NAME;
SELECT 
  "database_name",
  "schema_name",
  "name" AS sequence_name,
  GET_DDL('SEQUENCE', "database_name" || '.' || "schema_name" || '.' || "name") AS sequence_ddl
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

