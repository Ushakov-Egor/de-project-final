INSERT INTO {table_name} (
    schema_name, 
    table_name, 
    low_threshold,
    high_threshold,
    load_end, 
    status, 
    error_message
)
VALUES (
    :schema_name,
    :table_name,
    CAST(:low_threshold AS timestamp),
    CAST(:high_threshold AS timestamp), 
    CAST(:load_end AS timestamp), 
    :status, 
    :error_message
);