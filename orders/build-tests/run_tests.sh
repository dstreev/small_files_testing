#!/usr/bin/env bash

DB=${DB:-order}
SRC_LOC=/user/${USER}/datasets/${DB}

#auto_gather=("false" "true")
auto_gather=("false")

# Array of File Types
file_types=("ORC")
#file_types=("ORC" "TEXTFILE" "PARQUET" "AVRO")

# Array of DB Locations
#db_locations=("hdfs|/warehouse/tablespace/external/hive/|/warehouse/tablespace/managed/hive/")
#db_locations=("ofs-fso|ofs://OHOME90/warehouse/external-fso/hive/|ofs://OHOME90/warehouse/managed-fso/hive/" "hdfs|/warehouse/tablespace/external/hive/|/warehouse/tablespace/managed/hive/" "ofs|ofs://OHOME90/warehouse/external/hive/|ofs://OHOME90/warehouse/managed/hive/")
db_locations=("ofs-fso|ofs://OHOME90/warehouse/external-fso/hive/|ofs://OHOME90/warehouse/managed-fso/hive/" "hdfs|/warehouse/tablespace/external/hive/|/warehouse/tablespace/managed/hive/" "ofs|ofs://OHOME90/warehouse/external/hive/|ofs://OHOME90/warehouse/managed/hive/")

# Legacy Settings
legacies=("true" "false")

# ACID Types
#acid_insert_only_on=("true" "false")
acid_insert_only_on=("true")

# Stats on/off
for ag in ${auto_gather[@]}; do
  # Loop through File Types
  for ft in ${file_types[@]}; do
    # Loop through storage locations
    for locations in ${db_locations[@]}; do
      # split sub-list if available
      unset storage_id
      unset external_db_location
      unset managed_db_location
      unset external_scratchdir
      unset managed_scratchdir
      if [[ $locations == *"|"* ]]; then
        # split server name from sub-list
        tmpServerArray=(${locations//|/ })
        storage_id="${tmpServerArray[0]}"
        external_db_location="${tmpServerArray[1]}${DB}_${ft}.db"
        managed_db_location="${tmpServerArray[2]}${DB}_${ft}.db"
        external_scratchdir="${tmpServerArray[1]}/scratch_dir"
        managed_scratchdir="${tmpServerArray[2]}/scratch_dir"
      fi

      for LEGACY in ${legacies[@]}; do
        unset TABLE_TYPE
        if [[ ${LEGACY} = 'true' ]]; then
          TABLE_TYPE=external
          echo "===============  LEGACY  ================="
          echo "Job Variables: "
          echo "DB->${DB} TABLE_TYPE->${TABLE_TYPE} STATS_AUTO_GATHER_ENABLED->${ag} FILE_TYPE->${ft} SRC_LOC->${ORDER_SRC_LOC}"
          echo "External Location: ${external_db_location}"
          echo "Managed Location: ${managed_db_location}"
          if [[ ${SKIP} != true ]]; then
            hive -f orders.sql \
              --hiveconf hive.exec.scratchdir=${external_scratchdir} \
              --hiveconf hive.create.as.external.legacy=true \
              --hiveconf hive.stats.autogather=${ag} \
              --hiveconf hive.stats.column.autogather=${ag} \
              --hivevar DB=${DB} --hivevar FILE_TYPE=${ft} \
              --hivevar DB_EXT_LOC=${external_db_location} \
              --hivevar DB_MNGD_LOC=${managed_db_location} \
              --hivevar SRC_LOC=${SRC_LOC}
            # Get FS Details and append to Beeline Session Recording
            echo "DB Location Details " >>results/beeline_session.out
            hdfs dfs -count -h ${external_db_location}/* >>results/beeline_session.out 2>&1
            hdfs dfs -count -h ${managed_db_location}/* >>results/beeline_session.out 2>&1

            # Run the DB Cleanup and append to session output
            echo "DB Cleanup" >>results/beeline_session.out
            hive -f orders_cleanup.sql --hivevar DB=${DB} --hivevar FILE_TYPE=${ft}
            cat results/bl_cleanup.out >>results/beeline_session.out
            # mv beeline recording session.
            mv results/beeline_session.out results/bl_SL${storage_id}_TT${TABLE_TYPE}_S${ag}_${ft}_$(date '+%Y-%m-%d_%H-%M-%S').out
          fi
        else
          for acid_insert_only in ${acid_insert_only_on[@]}; do
            echo "==================================="
            unset acid_full
            unset TABLE_TYPE
            if [[ ${acid_insert_only} == 'true' ]]; then
              TABLE_TYPE=acid_insert
              acid_full=false
            else
              TABLE_TYPE=acid_full
              acid_full=true
            fi
            # Check for incompatible types.
            # If acid_full and non-ORC, skip.
            if [[ ${acid_full} == true && "${ft}" != "ORC" ]]; then
              echo "Skipping: ${TABLE_TYPE}->${ft}"
              break
            fi
            echo "Job Variables: "
            echo "DB->${DB} TABLE_TYPE->${TABLE_TYPE} STATS_AUTO_GATHER_ENABLED->${ag} FILE_TYPE->${ft} SRC_LOC->${ORDER_SRC_LOC}"
            echo "External Location: ${external_db_location}"
            echo "Managed Location: ${managed_db_location}"
            if [[ ${SKIP} != true ]]; then
              hive -f orders.sql \
                --hiveconf hive.exec.scratchdir=${managed_scratchdir} \
                --hiveconf hive.create.as.external.legacy=false \
                --hiveconf hive.create.as.acid=${acid_full} \
                --hiveconf hive.create.as.insert.only=${acid_insert_only} \
                --hiveconf hive.stats.autogather=${ag} \
                --hiveconf hive.stats.column.autogather=${ag} \
                --hivevar DB=${DB} --hivevar FILE_TYPE=${ft} \
                --hivevar DB_EXT_LOC=${external_db_location} \
                --hivevar DB_MNGD_LOC=${managed_db_location} \
                --hivevar SRC_LOC=${SRC_LOC}
              # Get FS Details and append to Beeline Session Recording
              echo "DB Location Details " >>results/beeline_session.out
              hdfs dfs -count -h ${external_db_location}/* >>results/beeline_session.out 2>&1
              hdfs dfs -count -h ${managed_db_location}/* >>results/beeline_session.out 2>&1

              # Run the DB Cleanup and append to session output
              echo "DB Cleanup" >>results/beeline_session.out
              hive -f orders_cleanup.sql --hivevar DB=${DB} --hivevar FILE_TYPE=${ft}
              cat results/bl_cleanup.out >>results/beeline_session.out
              # mv beeline recording session.
              mv results/beeline_session.out results/bl_SL${storage_id}_TT${TABLE_TYPE}_S${ag}_${ft}_$(date '+%Y-%m-%d_%H-%M-%S').out
            fi
          done
        fi
      done
    done
  done
done
