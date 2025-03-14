BEGIN
    LET rs RESULTSET := (SELECT GET_DDL('SCHEMA', schema_name) AS ddl
                         FROM information_schema.schemata
                         WHERE catalog_name = 'PROD_DATABASE_NAME');
    LET cur CURSOR FOR rs;
    OPEN cur;
    FOR row_variable IN cur DO
        -- Output or store the DDL for each schema
        RETURN row_variable.ddl;
    END FOR;
END;
