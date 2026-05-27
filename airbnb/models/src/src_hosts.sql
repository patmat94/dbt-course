WITH raw_hosts AS (
    select * from AIRBNB.RAW.RAW_HOSTS
)

SELECT
    id AS host_id,
    name AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM raw_hosts