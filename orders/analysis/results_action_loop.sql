USE ${DB};

-- Run for the following:
-- DEFAULT ORDER/ORDER_ITEM Build out
-- DEFAULT ORDER/ORDER_ITEM QUERY
-- DROP DATABASE to ensure clean run
-- SMALL-FILE ORDER/ORDER_ITEM QUERY
-- SMALL_FILE ORDER/ORDER_ITEM Build out

SELECT "${ACTION}";

INSERT
INTO
    METRICS_BREAKDOWN PARTITION (DATASET) (SELECT
                           O_RANGE.ORIGIN
                         , "${ACTION}"
                         , M.METRIC
                         , M.VALUE
                        , O_RANGE.DATASET
                       FROM
                           METRICS M
                               INNER JOIN
                               (SELECT
                                    O.ORIGIN
                                  , MIN_LINE_NUM
                                  , MAX_LINE_NUM
                                , O.DATASET
                                FROM
                                    ORIGIN O
                                        INNER JOIN (SELECT
                                                        ORIGIN
                                                      , MIN(LINE_NUM) AS MIN_LINE_NUM
                                                      , MAX(LINE_NUM) AS MAX_LINE_NUM
                                                    , DATASET
                                                    FROM
                                                        ACTIONS
                                                    WHERE
                                                        LOCATE("${ACTION}", ACTION) > 0
                                                    GROUP BY
                                                        ORIGIN, DATASET) ACTION_RANGE ON O.DATASET = ACTION_RANGE.DATASET AND
                                                                                O.ORIGIN = ACTION_RANGE.ORIGIN) O_RANGE
                               ON M.DATASET = O_RANGE.DATASET AND
                                   M.ORIGIN = O_RANGE.ORIGIN AND
                                  M.LINE_NUM BETWEEN O_RANGE.MIN_LINE_NUM AND O_RANGE.MAX_LINE_NUM);

SELECT * FROM METRICS_BREAKDOWN LIMIT 10;

-- Run for the following:
-- DEFAULT ORDER/ORDER_ITEM Build out
-- DEFAULT ORDER/ORDER_ITEM QUERY
-- DROP DATABASE to ensure clean run
-- SMALL-FILE ORDER/ORDER_ITEM QUERY
-- SMALL_FILE ORDER/ORDER_ITEM Build out

INSERT
INTO
    ACTION_DURATION PARTITION (DATASET) (SELECT
                         ORIGIN
                       , "${ACTION}"
                       , MIN(ACTION_TS)
                       , MAX(ACTION_TS)
                                         , DATASET
                     FROM
                         ACTIONS
                     WHERE
                         LOCATE("${ACTION}", ACTION) > 0
                     GROUP BY
                         ORIGIN, "${ACTION}", DATASET)
;


SELECT * FROM ACTION_DURATION LIMIT 10;