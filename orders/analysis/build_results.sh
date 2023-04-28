#!/usr/bin/env bash

DB=${DB:-order_results}
hive -f results_ddl.sql --hivevar DB=${DB}

hive -f results_parsing.sql --hivevar DB=${DB}

# -- DEFAULT ORDER/ORDER_ITEM Build out
# -- DEFAULT ORDER/ORDER_ITEM QUERY
# -- DROP DATABASE to ensure clean run
# -- SMALL-FILE ORDER/ORDER_ITEM QUERY
# -- SMALL_FILE ORDER/ORDER_ITEM Build out

ACTIONS=("DEFAULT ORDER/ORDER_ITEM Build out" "DEFAULT ORDER/ORDER_ITEM QUERY" "DROP DATABASE to ensure clean run" "SMALL-FILE ORDER/ORDER_ITEM QUERY" "SMALL_FILE ORDER/ORDER_ITEM Build out")
for (( i = 0; i < ${#ACTIONS[@]}; i++)); do
    hive -f results_action_loop.sql --hivevar DB=${DB} --hivevar ACTION="${ACTIONS[$i]}"
done