-- Generate all transfer commands in one query
WITH owned_objects AS (
  SELECT 
    object_type,
    object_name,
    object_schema,
    table_catalog AS database_name
  FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_PRIVILEGES
  WHERE privilege = 'OWNERSHIP'
    AND grantee = 'ROLE_A'
    AND deleted_on IS NULL
)
SELECT 
  CASE 
    WHEN object_type IN ('DATABASE', 'SCHEMA') THEN
      'GRANT OWNERSHIP ON ' || object_type || ' ' || object_name || 
      ' TO ROLE ROLE_B REVOKE CURRENT GRANTS;'
    ELSE
      'GRANT OWNERSHIP ON ' || object_type || ' ' || object_name || 
      ' IN ' || COALESCE(database_name||'.','') || COALESCE(object_schema,'') || 
      ' TO ROLE ROLE_B REVOKE CURRENT GRANTS;'
  END AS transfer_command
FROM owned_objects
ORDER BY 
  CASE object_type
    WHEN 'DATABASE' THEN 1
    WHEN 'SCHEMA' THEN 2
    ELSE 3
  END;
