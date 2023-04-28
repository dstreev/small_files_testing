!record results/beeline_session.out

-- External and Managed Table Creation control via the session setting:
-- The following settings should be control via --hiveconf in startup of beeline session.
-- NOTE: When 'true', overrides both 'hive.create.as.acid' and 'hive.create.as.insert.only'
SET hive.create.as.external.legacy;
-- NOTE: When 'true', this overrides 'hive.create.as.insert.only'
SET hive.create.as.acid;
SET hive.create.as.insert.only;
SET hive.stats.autogather;
SET hive.stats.column.autogather;
-- May effect the Move Operation.
SET hive.exec.scratchdir;

-- Turn of Results Cache for Managed Table Queries.
set hive.query.results.cache.enabled=false;

-- output vars.
SELECT "${DB}" AS DB, "${FILE_TYPE}" AS FILE_TYPE;
-- SELECT 'START DROP DATABASE to ensure clean run', `current_timestamp`();
-- DROP DATABASE IF EXISTS ${DB}_${FILE_TYPE} CASCADE;
-- SELECT 'END DROP DATABASE to ensure clean run', `current_timestamp`();
CREATE DATABASE IF NOT EXISTS ${DB} LOCATION "${DB_EXT_LOC}" MANAGEDLOCATION "${DB_MNGD_LOC}";
USE ${DB};
-- ALTER DATABASE ${DB}_${FILE_TYPE} SET LOCATION "${DB_EXT_LOC}";
DESCRIBE DATABASE ${DB};

-- Set TEZ Defaults to ensure session is clean
SET tez.grouping.min-size=52428800;
SET tez.grouping.max-size=1073741824;
SET tez.grouping.split-waves=1.7f;

-- Build the Source Table (Orders), which includes an ARRAY of the Order Items (struct).
DROP TABLE IF EXISTS ORDER_SRC;
CREATE EXTERNAL TABLE ORDER_SRC
(
    ID          STRING,
    USER_ID     STRING,
    ORDER_DATE  DATE,
    STATUS      STRING,
    -- Beware of STRUCT case.  It must match the json to pick it up.
    ORDER_ITEMS ARRAY<STRUCT<order_item_id :STRING, product_id :STRING, quantity :BIGINT, cost :DOUBLE>>
)
    STORED AS JSONFILE
    LOCATION '${SRC_LOC}';
select * from order_src limit 10;

-- Consumer tables types (EXTERNAL, ACID-FULL, ACID-INSERT_ONLY)
--      creation is controlled by the following session vars.
-- hive.create.as.external.legacy;
-- hive.create.as.insert.only;

-- Create Consumer Tables.
DROP TABLE IF EXISTS ORDER_${FILE_TYPE};
CREATE TABLE ORDER_${FILE_TYPE}
(
    ID          STRING,
    USER_ID     STRING,
    ORDER_DATE  DATE,
    STATUS      STRING
)
    STORED AS ${FILE_TYPE}
TBLPROPERTIES ('external.table.purge' = 'true');

-- Validate Create
SHOW CREATE TABLE ORDER_${FILE_TYPE};

DROP TABLE IF EXISTS ORDER_SMALL_${FILE_TYPE};
CREATE TABLE ORDER_SMALL_${FILE_TYPE}
(
    ID          STRING,
    USER_ID     STRING,
    ORDER_DATE  DATE,
    STATUS      STRING
)
    STORED AS ${FILE_TYPE}
TBLPROPERTIES ('external.table.purge' = 'true');

-- Validate Create
SHOW CREATE TABLE ORDER_SMALL_${FILE_TYPE};

DROP TABLE IF EXISTS ORDER_ITEM_${FILE_TYPE};
CREATE TABLE ORDER_ITEM_${FILE_TYPE}
(
    ORDER_ID      STRING,
    ORDER_ITEM_ID STRING,
    PRODUCT_ID    STRING,
    QUANTITY      BIGINT,
    COST          DOUBLE
)
    STORED AS ${FILE_TYPE}
    TBLPROPERTIES ('external.table.purge' = 'true');

-- Validate Create
SHOW CREATE TABLE ORDER_ITEM_${FILE_TYPE};

DROP TABLE IF EXISTS ORDER_ITEM_SMALL_${FILE_TYPE};
CREATE TABLE ORDER_ITEM_SMALL_${FILE_TYPE}
(
    ORDER_ID      STRING,
    ORDER_ITEM_ID STRING,
    PRODUCT_ID    STRING,
    QUANTITY      BIGINT,
    COST          DOUBLE
)
    STORED AS ${FILE_TYPE}
    TBLPROPERTIES ('external.table.purge' = 'true');

-- Validate Create
SHOW CREATE TABLE ORDER_ITEM_SMALL_${FILE_TYPE};

-- TRUNCATE TABLE ORDER_${FILE_TYPE};
-- TRUNCATE TABLE ORDER_SMALL_${FILE_TYPE};
-- TRUNCATE TABLE ORDER_ITEM_${FILE_TYPE};
-- TRUNCATE TABLE ORDER_ITEM_SMALL_${FILE_TYPE};

SELECT "SETTING TEZ DEFAULTS";

-- Defaults
SET tez.grouping.min-size=52428800;
SET tez.grouping.max-size=1073741824;
SET tez.grouping.split-waves=1.7f;

SELECT 'START EXPLAIN DEFAULT ORDER/ORDER_ITEM Build out', `current_timestamp`();
-- Build Order/Order_Item tables with default TEZ settings.
-- Note that since the 'source' is a json file, which is VERY bloated, we may find it helpful to
--    increase the tez settings to combine these files so we get better distribution of the source.
--    But we have to weight the differences here between the 'order' and the 'order_item'.  Since we
--    are used mti (multi-table-inserts), the same number of writers will be used for each.
--    Which could mean that the 'best' option would be to populate each separately or introduce some
--      addition reducer to each of the insert statements to control the final write size.
EXPLAIN EXTENDED FROM
    ORDER_SRC
INSERT
INTO TABLE
    ORDER_${FILE_TYPE}
SELECT
    ID
    ,USER_ID
    ,ORDER_DATE
    ,STATUS
INSERT INTO TABLE
    ORDER_ITEM_${FILE_TYPE}
SELECT
    ID
  , OI.order_item_id
  , OI.product_id
  , OI.quantity
  , OI.cost LATERAL VIEW EXPLODE(order_items) MI AS OI;

SELECT 'END EXPLAIN DEFAULT ORDER/ORDER_ITEM Build out', `current_timestamp`();

SELECT 'START DEFAULT ORDER/ORDER_ITEM Build out', `current_timestamp`();
-- Build Order/Order_Item tables with default TEZ settings.
-- Note that since the 'source' is a json file, which is VERY bloated, we may find it helpful to
--    increase the tez settings to combine these files so we get better distribution of the source.
--    But we have to weight the differences here between the 'order' and the 'order_item'.  Since we
--    are used mti (multi-table-inserts), the same number of writers will be used for each.
--    Which could mean that the 'best' option would be to populate each separately or introduce some
--      addition reducer to each of the insert statements to control the final write size.
FROM
    ORDER_SRC
INSERT
INTO TABLE
    ORDER_${FILE_TYPE}
SELECT
    ID
    ,USER_ID
    ,ORDER_DATE
    ,STATUS
INSERT INTO TABLE
    ORDER_ITEM_${FILE_TYPE}
SELECT
    ID
  , OI.order_item_id
  , OI.product_id
  , OI.quantity
  , OI.cost LATERAL VIEW EXPLODE(order_items) MI AS OI;

SELECT 'END DEFAULT ORDER/ORDER_ITEM Build out', `current_timestamp`();


SELECT "SETTING TEZ SMALL-FILE CONFIGS";

-- Create Small Files
-- SET tez.grouping.min-size=50000;
-- SET tez.grouping.max-size=100000;
-- SET tez.grouping.split-waves=1000;
SET tez.grouping.min-size=50000;
--SET tez.grouping.max-size=100000;
SET tez.grouping.max-size=1000000;
SET tez.grouping.split-waves=150;


SELECT 'START EXPLAIN SMALL_FILE ORDER/ORDER_ITEM Build out', `current_timestamp`();

-- Build Order/Order_Item tables with Small Files by manipulating the tez settings.
EXPLAIN EXTENDED FROM
    ORDER_SRC
INSERT
INTO TABLE
    ORDER_SMALL_${FILE_TYPE}
SELECT
    ID
    ,USER_ID
    ,ORDER_DATE
    ,STATUS
INSERT INTO TABLE
    ORDER_ITEM_SMALL_${FILE_TYPE}
SELECT
    ID
  , OI.order_item_id
  , OI.product_id
  , OI.quantity
  , OI.cost LATERAL VIEW EXPLODE(order_items) MI AS OI;

SELECT 'END EXPLAIN SMALL_FILE ORDER/ORDER_ITEM Build out', `current_timestamp`();

SELECT 'START SMALL_FILE ORDER/ORDER_ITEM Build out', `current_timestamp`();

-- Build Order/Order_Item tables with Small Files by manipulating the tez settings.
FROM
    ORDER_SRC
INSERT
INTO TABLE
    ORDER_SMALL_${FILE_TYPE}
SELECT
    ID
    ,USER_ID
    ,ORDER_DATE
    ,STATUS
INSERT INTO TABLE
    ORDER_ITEM_SMALL_${FILE_TYPE}
SELECT
    ID
  , OI.order_item_id
  , OI.product_id
  , OI.quantity
  , OI.cost LATERAL VIEW EXPLODE(order_items) MI AS OI;

SELECT 'END SMALL_FILE ORDER/ORDER_ITEM Build out', `current_timestamp`();

SELECT 'Set tez.grouping.* to DEFAULTS', `current_timestamp`();
-- Defaults
SET tez.grouping.min-size=52428800;
SET tez.grouping.max-size=1073741824;
SET tez.grouping.split-waves=1.7f;

!run orders_select.sql

SELECT 'Set tez.grouping.* to min 8Mb, max 16Mb, and waves 100', `current_timestamp`();
-- Perf Adjustments for Small Files.
SET tez.grouping.min-size=8000000;
SET tez.grouping.max-size=16000000;
SET tez.grouping.split-waves=100;

!run orders_select.sql

!record
-- Clean up at end of run.
-- But may want to keep around to review files.
-- SELECT 'START DROP DATABASE to ensure clean run next time', `current_timestamp`();
-- DROP DATABASE IF EXISTS ${DB}_${FILE_TYPE} CASCADE;
-- SELECT 'END DROP DATABASE to ensure clean run next time', `current_timestamp`();
