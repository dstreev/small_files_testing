DROP DATABASE IF EXISTS ${DB} CASCADE;
CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

SET hive.exec.scratchdir;

-- Source Table
DROP TABLE IF EXISTS CARTESIAN_SRC;
CREATE EXTERNAL TABLE CARTESIAN_SRC
(
    ID     STRING,
    CODE   STRING,
    FIELD1 STRING,
    LEFTS  ARRAY<STRUCT<id : STRING, code : STRING, cost : INT, field_001 : STRING, field_002 : STRING, field_003
                        : STRING, field_004 : STRING, field_005 : STRING, field_006 : STRING, field_007 : STRING,
                        field_008 : STRING, field_009 : STRING, field_010 : STRING, field_011 : STRING, field_012
                        : STRING, field_013 : STRING, field_014 : STRING, field_015 : STRING, field_016 : STRING,
                        field_017 : STRING, field_018 : STRING, field_019 : STRING, field_020 : STRING, field_021
                        : STRING, field_022 : STRING, field_023 : STRING, field_024 : STRING, field_025 : STRING,
                        field_026 : STRING, field_027 : STRING, field_028 : STRING, field_029 : STRING, field_030
                        : STRING, field_031 : STRING, field_032 : STRING, field_033 : STRING, field_034 : STRING,
                        field_035 : STRING, field_036 : STRING, field_037 : STRING, field_038 : STRING, field_039
                        : STRING, field_040 : STRING, field_041 : STRING, field_042 : STRING, field_043 : STRING,
                        field_044 : STRING, field_045 : STRING, field_046 : STRING, field_047 : STRING, field_048
                        : STRING, field_049 : STRING, field_050 : STRING, field_051 : STRING, field_052 : STRING,
                        field_053 : STRING, field_054 : STRING, field_055 : STRING, field_056 : STRING, field_057
                        : STRING, field_058 : STRING, field_059 : STRING, field_060 : STRING, field_061 : STRING,
                        field_062 : STRING, field_063 : STRING, field_064 : STRING, field_065 : STRING, field_066
                        : STRING, field_067 : STRING, field_068 : STRING, field_069 : STRING, field_070 : STRING,
                        field_071 : STRING, field_072 : STRING, field_073 : STRING, field_074 : STRING, field_075
                        : STRING, field_076 : STRING, field_077 : STRING, field_078 : STRING, field_079 : STRING,
                        field_080 : STRING, field_081 : STRING, field_082 : STRING, field_083 : STRING, field_084
                        : STRING, field_085 : STRING, field_086 : STRING, field_087 : STRING, field_088 : STRING,
                        field_089 : STRING, field_090 : STRING, field_091 : STRING, field_092 : STRING, field_093
                        : STRING, field_094 : STRING, field_095 : STRING, field_096 : STRING, field_097 : STRING,
                        field_098 : STRING, field_099 : STRING>>,
    RIGHTS ARRAY<STRUCT<id : STRING, code : STRING, cost : INT, field_001 : STRING, field_002 : STRING, field_003
                        : STRING, field_004 : STRING, field_005 : STRING, field_006 : STRING, field_007 : STRING,
                        field_008 : STRING, field_009 : STRING, field_010 : STRING, field_011 : STRING, field_012
                        : STRING, field_013 : STRING, field_014 : STRING, field_015 : STRING, field_016 : STRING,
                        field_017 : STRING, field_018 : STRING, field_019 : STRING, field_020 : STRING, field_021
                        : STRING, field_022 : STRING, field_023 : STRING, field_024 : STRING, field_025 : STRING,
                        field_026 : STRING, field_027 : STRING, field_028 : STRING, field_029 : STRING, field_030
                        : STRING, field_031 : STRING, field_032 : STRING, field_033 : STRING, field_034 : STRING,
                        field_035 : STRING, field_036 : STRING, field_037 : STRING, field_038 : STRING, field_039
                        : STRING, field_040 : STRING, field_041 : STRING, field_042 : STRING, field_043 : STRING,
                        field_044 : STRING, field_045 : STRING, field_046 : STRING, field_047 : STRING, field_048
                        : STRING, field_049 : STRING, field_050 : STRING, field_051 : STRING, field_052 : STRING,
                        field_053 : STRING, field_054 : STRING, field_055 : STRING, field_056 : STRING, field_057
                        : STRING, field_058 : STRING, field_059 : STRING, field_060 : STRING, field_061 : STRING,
                        field_062 : STRING, field_063 : STRING, field_064 : STRING, field_065 : STRING, field_066
                        : STRING, field_067 : STRING, field_068 : STRING, field_069 : STRING, field_070 : STRING,
                        field_071 : STRING, field_072 : STRING, field_073 : STRING, field_074 : STRING, field_075
                        : STRING, field_076 : STRING, field_077 : STRING, field_078 : STRING, field_079 : STRING,
                        field_080 : STRING, field_081 : STRING, field_082 : STRING, field_083 : STRING, field_084
                        : STRING, field_085 : STRING, field_086 : STRING, field_087 : STRING, field_088 : STRING,
                        field_089 : STRING, field_090 : STRING, field_091 : STRING, field_092 : STRING, field_093
                        : STRING, field_094 : STRING, field_095 : STRING, field_096 : STRING, field_097 : STRING,
                        field_098 : STRING, field_099 : STRING>>
)
    STORED AS JSONFILE
    LOCATION '${SRC_LOC}';

SELECT * FROM CARTESIAN_SRC LIMIT 1;


DROP TABLE IF EXISTS ENTITY_${FILE_TYPE};
CREATE EXTERNAL TABLE IF NOT EXISTS ENTITY_${FILE_TYPE} (
    ID STRING,
    CODE STRING,
    FIELD1 STRING
) STORED AS ${FILE_TYPE};

DROP TABLE IF EXISTS LEFT_${FILE_TYPE};
CREATE EXTERNAL TABLE LEFT_${FILE_TYPE}
(
    ENTITY_ID          STRING,
    ID     STRING,
    CODE STRING,
    COST INT,
    field_001 STRING,
    field_002 STRING,
    field_003 STRING,
    field_004 STRING,
    field_005 STRING,
    field_006 STRING,
    field_007 STRING,
    field_008 STRING,
    field_009 STRING,
    field_010 STRING,
    field_011 STRING,
    field_012 STRING,
    field_013 STRING,
    field_014 STRING,
    field_015 STRING,
    field_016 STRING,
    field_017 STRING,
    field_018 STRING,
    field_019 STRING,
    field_020 STRING,
    field_021 STRING,
    field_022 STRING,
    field_023 STRING,
    field_024 STRING,
    field_025 STRING,
    field_026 STRING,
    field_027 STRING,
    field_028 STRING,
    field_029 STRING,
    field_030 STRING,
    field_031 STRING,
    field_032 STRING,
    field_033 STRING,
    field_034 STRING,
    field_035 STRING,
    field_036 STRING,
    field_037 STRING,
    field_038 STRING,
    field_039 STRING,
    field_040 STRING,
    field_041 STRING,
    field_042 STRING,
    field_043 STRING,
    field_044 STRING,
    field_045 STRING,
    field_046 STRING,
    field_047 STRING,
    field_048 STRING,
    field_049 STRING,
    field_050 STRING,
    field_051 STRING,
    field_052 STRING,
    field_053 STRING,
    field_054 STRING,
    field_055 STRING,
    field_056 STRING,
    field_057 STRING,
    field_058 STRING,
    field_059 STRING,
    field_060 STRING,
    field_061 STRING,
    field_062 STRING,
    field_063 STRING,
    field_064 STRING,
    field_065 STRING,
    field_066 STRING,
    field_067 STRING,
    field_068 STRING,
    field_069 STRING,
    field_070 STRING,
    field_071 STRING,
    field_072 STRING,
    field_073 STRING,
    field_074 STRING,
    field_075 STRING,
    field_076 STRING,
    field_077 STRING,
    field_078 STRING,
    field_079 STRING,
    field_080 STRING,
    field_081 STRING,
    field_082 STRING,
    field_083 STRING,
    field_084 STRING,
    field_085 STRING,
    field_086 STRING,
    field_087 STRING,
    field_088 STRING,
    field_089 STRING,
    field_090 STRING,
    field_091 STRING,
    field_092 STRING,
    field_093 STRING,
    field_094 STRING,
    field_095 STRING,
    field_096 STRING,
    field_097 STRING,
    field_098 STRING,
    field_099 STRING
)
    STORED AS ${FILE_TYPE};

DROP TABLE IF EXISTS RIGHT_${FILE_TYPE};
CREATE EXTERNAL TABLE RIGHT_${FILE_TYPE}
(
    ENTITY_ID          STRING,
    ID     STRING,
    CODE STRING,
    COST INT,
    field_001 STRING,
    field_002 STRING,
    field_003 STRING,
    field_004 STRING,
    field_005 STRING,
    field_006 STRING,
    field_007 STRING,
    field_008 STRING,
    field_009 STRING,
    field_010 STRING,
    field_011 STRING,
    field_012 STRING,
    field_013 STRING,
    field_014 STRING,
    field_015 STRING,
    field_016 STRING,
    field_017 STRING,
    field_018 STRING,
    field_019 STRING,
    field_020 STRING,
    field_021 STRING,
    field_022 STRING,
    field_023 STRING,
    field_024 STRING,
    field_025 STRING,
    field_026 STRING,
    field_027 STRING,
    field_028 STRING,
    field_029 STRING,
    field_030 STRING,
    field_031 STRING,
    field_032 STRING,
    field_033 STRING,
    field_034 STRING,
    field_035 STRING,
    field_036 STRING,
    field_037 STRING,
    field_038 STRING,
    field_039 STRING,
    field_040 STRING,
    field_041 STRING,
    field_042 STRING,
    field_043 STRING,
    field_044 STRING,
    field_045 STRING,
    field_046 STRING,
    field_047 STRING,
    field_048 STRING,
    field_049 STRING,
    field_050 STRING,
    field_051 STRING,
    field_052 STRING,
    field_053 STRING,
    field_054 STRING,
    field_055 STRING,
    field_056 STRING,
    field_057 STRING,
    field_058 STRING,
    field_059 STRING,
    field_060 STRING,
    field_061 STRING,
    field_062 STRING,
    field_063 STRING,
    field_064 STRING,
    field_065 STRING,
    field_066 STRING,
    field_067 STRING,
    field_068 STRING,
    field_069 STRING,
    field_070 STRING,
    field_071 STRING,
    field_072 STRING,
    field_073 STRING,
    field_074 STRING,
    field_075 STRING,
    field_076 STRING,
    field_077 STRING,
    field_078 STRING,
    field_079 STRING,
    field_080 STRING,
    field_081 STRING,
    field_082 STRING,
    field_083 STRING,
    field_084 STRING,
    field_085 STRING,
    field_086 STRING,
    field_087 STRING,
    field_088 STRING,
    field_089 STRING,
    field_090 STRING,
    field_091 STRING,
    field_092 STRING,
    field_093 STRING,
    field_094 STRING,
    field_095 STRING,
    field_096 STRING,
    field_097 STRING,
    field_098 STRING,
    field_099 STRING
)
    STORED AS ${FILE_TYPE};

set hive.stats.autogather=false;
set hive.stats.autogather.columns=false;

FROM
    CARTESIAN_SRC
INSERT OVERWRITE
TABLE
    ENTITY_${FILE_TYPE}
SELECT
    ID
     ,CODE
     ,FIELD1
INSERT OVERWRITE TABLE
    LEFT_${FILE_TYPE}
SELECT
    ID
     , OI.id AS ID
     , OI.code
     , OI.cost
     , OI.field_001
     , OI.field_002
     , OI.field_003
     , OI.field_004
     , OI.field_005
     , OI.field_006
     , OI.field_007
     , OI.field_008
     , OI.field_009
     , OI.field_010
     , OI.field_011
     , OI.field_012
     , OI.field_013
     , OI.field_014
     , OI.field_015
     , OI.field_016
     , OI.field_017
     , OI.field_018
     , OI.field_019
     , OI.field_020
     , OI.field_021
     , OI.field_022
     , OI.field_023
     , OI.field_024
     , OI.field_025
     , OI.field_026
     , OI.field_027
     , OI.field_028
     , OI.field_029
     , OI.field_030
     , OI.field_031
     , OI.field_032
     , OI.field_033
     , OI.field_034
     , OI.field_035
     , OI.field_036
     , OI.field_037
     , OI.field_038
     , OI.field_039
     , OI.field_040
     , OI.field_041
     , OI.field_042
     , OI.field_043
     , OI.field_044
     , OI.field_045
     , OI.field_046
     , OI.field_047
     , OI.field_048
     , OI.field_049
     , OI.field_050
     , OI.field_051
     , OI.field_052
     , OI.field_053
     , OI.field_054
     , OI.field_055
     , OI.field_056
     , OI.field_057
     , OI.field_058
     , OI.field_059
     , OI.field_060
     , OI.field_061
     , OI.field_062
     , OI.field_063
     , OI.field_064
     , OI.field_065
     , OI.field_066
     , OI.field_067
     , OI.field_068
     , OI.field_069
     , OI.field_070
     , OI.field_071
     , OI.field_072
     , OI.field_073
     , OI.field_074
     , OI.field_075
     , OI.field_076
     , OI.field_077
     , OI.field_078
     , OI.field_079
     , OI.field_080
     , OI.field_081
     , OI.field_082
     , OI.field_083
     , OI.field_084
     , OI.field_085
     , OI.field_086
     , OI.field_087
     , OI.field_088
     , OI.field_089
     , OI.field_090
     , OI.field_091
     , OI.field_092
     , OI.field_093
     , OI.field_094
     , OI.field_095
     , OI.field_096
     , OI.field_097
     , OI.field_098
     , OI.field_099
      LATERAL VIEW EXPLODE(LEFTS) MI AS OI
INSERT OVERWRITE TABLE
    RIGHT_${FILE_TYPE}
SELECT
    ID
     , OI.id AS ID
     , OI.code
     , OI.cost
     , OI.field_001
     , OI.field_002
     , OI.field_003
     , OI.field_004
     , OI.field_005
     , OI.field_006
     , OI.field_007
     , OI.field_008
     , OI.field_009
     , OI.field_010
     , OI.field_011
     , OI.field_012
     , OI.field_013
     , OI.field_014
     , OI.field_015
     , OI.field_016
     , OI.field_017
     , OI.field_018
     , OI.field_019
     , OI.field_020
     , OI.field_021
     , OI.field_022
     , OI.field_023
     , OI.field_024
     , OI.field_025
     , OI.field_026
     , OI.field_027
     , OI.field_028
     , OI.field_029
     , OI.field_030
     , OI.field_031
     , OI.field_032
     , OI.field_033
     , OI.field_034
     , OI.field_035
     , OI.field_036
     , OI.field_037
     , OI.field_038
     , OI.field_039
     , OI.field_040
     , OI.field_041
     , OI.field_042
     , OI.field_043
     , OI.field_044
     , OI.field_045
     , OI.field_046
     , OI.field_047
     , OI.field_048
     , OI.field_049
     , OI.field_050
     , OI.field_051
     , OI.field_052
     , OI.field_053
     , OI.field_054
     , OI.field_055
     , OI.field_056
     , OI.field_057
     , OI.field_058
     , OI.field_059
     , OI.field_060
     , OI.field_061
     , OI.field_062
     , OI.field_063
     , OI.field_064
     , OI.field_065
     , OI.field_066
     , OI.field_067
     , OI.field_068
     , OI.field_069
     , OI.field_070
     , OI.field_071
     , OI.field_072
     , OI.field_073
     , OI.field_074
     , OI.field_075
     , OI.field_076
     , OI.field_077
     , OI.field_078
     , OI.field_079
     , OI.field_080
     , OI.field_081
     , OI.field_082
     , OI.field_083
     , OI.field_084
     , OI.field_085
     , OI.field_086
     , OI.field_087
     , OI.field_088
     , OI.field_089
     , OI.field_090
     , OI.field_091
     , OI.field_092
     , OI.field_093
     , OI.field_094
     , OI.field_095
     , OI.field_096
     , OI.field_097
     , OI.field_098
     , OI.field_099
        LATERAL VIEW EXPLODE(RIGHTS) MI AS OI;


SELECT * FROM LEFT_${FILE_TYPE} LIMIT 100;
SELECT * FROM RIGHT_${FILE_TYPE} LIMIT 100;

