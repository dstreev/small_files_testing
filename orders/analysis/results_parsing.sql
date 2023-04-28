-- CREATE DATABASE IF NOT EXISTS ${DB};

USE ${DB};

-- Results RAW
SELECT *
FROM
    RESULTS_RAW
LIMIT 10;

-- Get out the metrics
INSERT
    OVERWRITE TABLE
    METRICS PARTITION (DATASET) (SELECT
                 ORIGIN
               , LINE_NUM
               , SPLIT(TRIM(CONTENT), ":")[0]
               , SPLIT(TRIM(CONTENT), ":")[1]
             , DATASET
             FROM
                 (SELECT
                      REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:", 1)               AS ORIGIN
                    , REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:([0-9]*)\:", 2)     AS LINE_NUM
                    , REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:([0-9]*)\:(.*)", 3) AS CONTENT
                    , NEW_LINE
                  , DATASET
                  FROM
                      (SELECT
                           REPLACE(LINE, ":INFO  ", "") AS NEW_LINE
                       , DATASET
                       FROM
                           RESULTS_RAW) NO_INFO) CNT
             WHERE
                   SUBSTR(CNT.CONTENT, 1, 1) != "|"
               AND LOCATE(":", CNT.CONTENT) > 0);

-- Get out the Run DAG metrics
INSERT
INTO
    METRICS PARTITION (DATASET) (SELECT
                 ORIGIN
               , LINE_NUM
               , SPLIT(TRIM(CONTENT), "DAG")[0]
               , SUBSTR(SPLIT(TRIM(CONTENT), "DAG")[1],0,LENGTH(SPLIT(TRIM(CONTENT), "DAG")[1])-1)
                                 , DATASET
             FROM
                 (SELECT
                      REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:", 1)               AS ORIGIN
                    , REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:([0-9]*)\:", 2)     AS LINE_NUM
                    , REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:([0-9]*)\:(.*)", 3) AS CONTENT
                    , NEW_LINE
                  , DATASET
                  FROM
                      (SELECT
                           REPLACE(LINE, ":INFO  ", "") AS NEW_LINE
                       , DATASET
                       FROM
                           RESULTS_RAW) NO_INFO) CNT
             WHERE
                   SUBSTR(CNT.CONTENT, 1, 1) != "|"
               AND LOCATE("DAG", CNT.CONTENT) > 0);

-- Metrics
SELECT *
FROM
    METRICS
LIMIT 100;


INSERT
    OVERWRITE TABLE
    ACTIONS PARTITION (DATASET) (SELECT
                 ORIGIN
               , LINE_NUM
               , SPLIT(TRIM(CONTENT), "\\|")[1] AS ACTION
               , SPLIT(TRIM(CONTENT), "\\|")[2] AS ACTION_TS
                                 , DATASET
             FROM
                 (SELECT
                      REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:", 1)               AS ORIGIN
                    , REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:([0-9]*)\:", 2)     AS LINE_NUM
                    , REGEXP_EXTRACT(NEW_LINE, "(.*)\.out\:([0-9]*)\:(.*)", 3) AS CONTENT
                    , NEW_LINE
                  , DATASET
                  FROM
                      (SELECT
                           REPLACE(LINE, ":INFO  ", "") AS NEW_LINE
                       , DATASET
                       FROM
                           RESULTS_RAW) NO_INFO) CNT
             WHERE
                 SUBSTR(CNT.CONTENT, 1, 1) = "|");

-- Distinct Actions
SELECT DISTINCT
    ACTION
FROM
    ACTIONS;

INSERT OVERWRITE TABLE
    ORIGIN PARTITION (DATASET) (SELECT DISTINCT
                ORIGIN
              , REGEXP_EXTRACT(ORIGIN, "bl_SL(.*)_TT(.*)_S(true|false)_(AVRO|ORC|PARQUET|TEXTFILE)_.*",
                               1) AS STORAGE_LOCATION
              , REGEXP_EXTRACT(ORIGIN, "bl_SL(.*)_TT(.*)_S(true|false)_(AVRO|ORC|PARQUET|TEXTFILE)_.*",
                               2) AS TABLE_TYPE
              , REGEXP_EXTRACT(ORIGIN, "bl_SL(.*)_TT(.*)_S(true|false)_(AVRO|ORC|PARQUET|TEXTFILE)_.*:",
                               3) AS STATS
              , REGEXP_EXTRACT(ORIGIN, "bl_SL(.*)_TT(.*)_S(true|false)_(AVRO|ORC|PARQUET|TEXTFILE)_.*",
                               4) AS FILE_TYPE
                                , DATASET
            FROM
                METRICS);

-- Origin
SELECT *
FROM
    ORIGIN;

SELECT DISTINCT
    ORIGIN
  , REGEXP_EXTRACT(ORIGIN, "bl_SL(.*)_TT(.*)_S(true|false)_(AVRO|ORC|PARQUET|TEXTFILE)_.*",
                   4) AS FILE_TYPE
  , DATASET
FROM
    METRICS LIMIT 10;