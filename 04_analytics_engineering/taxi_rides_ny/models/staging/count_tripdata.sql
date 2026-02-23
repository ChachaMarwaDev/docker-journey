-- models/count_tripdata.sql
WITH green AS (
    SELECT COUNT(*) AS green_count
    FROM {{ source('prod', 'green_tripdata') }}
),
yellow AS (
    SELECT COUNT(*) AS yellow_count
    FROM {{ source('prod', 'yellow_tripdata') }}
)
SELECT *
FROM green, yellow


-- count.sql
-- SELECT COUNT(*) FROM prod.green_tripdata;
-- SELECT COUNT(*) FROM prod.yellow_tripdata;
