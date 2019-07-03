#!/usr/bin/env bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( 
        DT \
        MODE \
        BIGQUERY_SOURCE \
        BIGQUERY_DESTINATION
      )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
	echo -e "\nUsage:\nfetch_normalized_vms YYYY-MM-DD BIGQUERY_SOURCE BIGQUERY_DESTINATION  \n"
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
  echo -e "DT: Date to be fetched in YYYY-MM-DD format.\n"
  echo -e "MODE: The mode how to fetch the data.\n"
  echo -e "BIGQUERY_SOURCE: BigQuery dataset and table where the input is already stored (Format expected <DATASET>.<TABLE>).\n"
  echo -e "BIGQUERY_DESTINATION: BigQuery dataset and table where will be stored the output (Format expected <DATASET>.<TABLE>).\n"

}

if [[ $# -ne ${#ARGS[@]} ]]
then
    display_usage
    exit 1
fi

ARG_VALUES=("$@")
PARAMS=()
for index in ${!ARGS[*]}; do
  echo "${ARGS[$index]}=${ARG_VALUES[$index]}"
  declare "${ARGS[$index]}"="${ARG_VALUES[$index]}"
done

SQL=${ASSETS}/fetch-${MODE}-normalized-vms.sql.j2

YYYYMMDD=$(yyyymmdd ${DT})
SOURCE_TABLE=${BIGQUERY_SOURCE/:/.}

DEST_TABLE=${BIGQUERY_DESTINATION}${YYYYMMDD}
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: VMS ${SOURCE_TABLE}" 
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )

echo "Fetching ${MODE} normalized vms into ${DEST_TABLE}..."
echo "${TABLE_DESC}"

jinja2 ${SQL} -D source=${SOURCE_TABLE} -D date=${DT}  \
  | bq query -q --max_rows=0 --allow_large_results \
    --replace \
    --nouse_legacy_sql \
    --destination_schema ${ASSETS}/normalized_schema.json \
    --destination_table ${DEST_TABLE} \

if [ "$?" -ne 0 ]; then
  echo "  Unable to run the normalize script for date ${DT}"
  exit 1
fi

echo "Updating table description ${DEST_TABLE}"

bq update --description "${TABLE_DESC}" ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to update the normalize table decription ${DT}"
  exit 1
fi

echo "${DEST_TABLE} Done."
