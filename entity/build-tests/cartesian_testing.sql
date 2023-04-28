USE ${DB};

show tables;

SHOW CREATE TABLE left_orc;

SELECT COUNT(1) FROM (SELECT L.ENTITY_ID,
                             L.ID,
                             R.ID
                      FROM LEFT_${FILE_TYPE} l
                               LEFT OUTER JOIN RIGHT_${FILE_TYPE} r ON
                          l.entity_id = r.entity_id) sub;

set hive.auto.convert.join.noconditionaltask=false;
set hive.auto.convert.join=false;

EXPLAIN ANALYZE FORMATTED
SELECT COUNT(1) FROM (
SELECT L.ENTITY_ID,
       L.ID,
       R.ID
FROM LEFT_${FILE_TYPE} l
         LEFT OUTER JOIN RIGHT_${FILE_TYPE} r ON
        l.entity_id = r.entity_id) sub;

WITH D_L AS (SELECT ENTITY_ID, ID FROM LEFT_${FILE_TYPE} CLUSTER BY ID);

SELECT COUNT(1) FROM (SELECT L.ENTITY_ID,
                             L.ID,
                             R.ID
                      FROM LEFT_${FILE_TYPE} L
                               LEFT OUTER JOIN RIGHT_${FILE_TYPE} R ON
                              L.entity_id = R.entity_id
                      CLUSTER BY L.ID) sub;

