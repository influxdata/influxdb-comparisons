#!/bin/bash

/bulk_data_gen -debug $DEBUG -format $FORMAT -interleaved-generation-groups $INTERLEAVED_GENERATION_GROUPS -scale-var $SCALE_VAR -seed $SEED -timestamp-end $TIMESTAMP_END -timestamp-start $TIMESTAMP_START -use-case $USE_CASE | /bulk_load_influx -urls $URLS
