USE ${DB};
show tables;
-- describe database orders_results;
-- use orders_results;
-- show tables;
-- drop table action_duration;
-- drop table actions;
-- drop table metrics;
-- drop table METRICS_BREAKDOWN;
-- drop table origin;
-- drop table results_raw;
-- show create table results_raw;
-- DROP DATABASE orders_results;
-- Tables
-- ORIGIN
--     ORIGIN           STRING,
--     STORAGE_LOCATION STRING,
--     LEGACY           BOOLEAN,
--     STATS            BOOLEAN,
--     FILE_TYPE        STRING
-- Origin
SELECT *
FROM
    ORIGIN
where dataset = '${DATASET}';
LIMIT 10;

-- METRICS_BREAKDOWN
--     ORIGIN STRING,
--     ACTION STRING,
--     METRIC STRING,
--     VALUE  BIGINT
-- Metrics Breakdown
SELECT *
FROM
    METRICS_BREAKDOWN
WHERE
    METRIC LIKE 'Ru%'
LIMIT 10;

-- ACTION_DURATION
--     ORIGIN   STRING,
--     ACTION   STRING,
--     START_TS TIMESTAMP,
--     END_TS   TIMESTAMP
-- Action Duration
SELECT *
FROM
    ACTION_DURATION
LIMIT 10;

-- Metrics
SELECT DISTINCT
    METRIC
FROM
    METRICS_BREAKDOWN
limit 10;
-- Action Groups
-- DEFAULT ORDER/ORDER_ITEM Build out
-- DEFAULT ORDER/ORDER_ITEM QUERY
-- DROP DATABASE to ensure clean run
-- SMALL-FILE ORDER/ORDER_ITEM QUERY
-- SMALL_FILE ORDER/ORDER_ITEM Build out

-- Buildout Move Time Comparison
-- Run with Actions: "DEFAULT ORDER/ORDER_ITEM Build out" and "SMALL_FILE ORDER/ORDER_ITEM Build out"
SELECT
    O.DATASET
  , O.STORAGE_LOCATION
  , O.TABLE_TYPE
--   , CONCAT(O.STORAGE_LOCATION, "-", O.TABLE_TYPE) AS                                SL_TT
  , O.FILE_TYPE
  , MB.ACTION
--   , CAST(AD.START_TS AS STRING)
  , AD.END_TS - AD.START_TS                                                     ACTION_DURATION
  , MB.METRIC
  , MB.VALUE                                  AS                                JOB_TIME
  , (unix_timestamp(AD.END_TS) - unix_timestamp(AD.START_TS)) - MB.VALUE        MOVE_TIME
  , ROUND((((unix_timestamp(AD.END_TS) - unix_timestamp(AD.START_TS)) - MB.VALUE) /
           (unix_timestamp(AD.END_TS) - unix_timestamp(AD.START_TS))) * 100, 2) MOVE_PERCENT
FROM
    ORIGIN O
        INNER JOIN METRICS_BREAKDOWN MB ON O.ORIGIN = MB.ORIGIN
        INNER JOIN ACTION_DURATION AD ON O.ORIGIN = AD.ORIGIN AND
                                         MB.ACTION = AD.ACTION
WHERE
      MB.ACTION = "${ACTION}"
  AND STATS = false
  AND MB.METRIC LIKE "Run%"
  AND O.FILE_TYPE LIKE "${FILE_TYPE}"
AND O.DATASET = "${DATASET}"
ORDER BY
    AD.END_TS - AD.START_TS ASC;

-- File Type Sizes
SELECT
    O.STORAGE_LOCATION
  , O.LEGACY AS EXTERNAL_TABLE_TYPE
--   , O.STATS
  , O.FILE_TYPE
  , MB.ACTION
--   , CAST(AD.START_TS AS STRING)
--   , AD.END_TS - AD.START_TS                                                     ACTION_DURATION
  , MB.METRIC
  , MB.VALUE
FROM
    ORIGIN O
        INNER JOIN METRICS_BREAKDOWN MB ON O.ORIGIN = MB.ORIGIN
        INNER JOIN ACTION_DURATION AD ON O.ORIGIN = AD.ORIGIN AND
                                         MB.ACTION = AD.ACTION
WHERE
      MB.ACTION = "${ACTION}"
  AND O.STATS = false
  AND MB.METRIC LIKE "%BYTES_WRITTEN"
  AND O.FILE_TYPE LIKE "${FILE_TYPE}"
ORDER BY
    AD.END_TS - AD.START_TS ASC;
