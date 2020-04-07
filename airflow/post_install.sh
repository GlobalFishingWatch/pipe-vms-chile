#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_vms_chile \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    events_dataset="{{ var.value.EVENTS_DATASET }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_dataset="VMS_Chile" \
    source_table="raw_all_view" \
    source_naf_dataset="VMS_Chile" \
    source_naf_table="raw_naf_all_view" \
    normalized="chile_vms_normalized_" \
    fleets=[ "aquaculture", "industry", "small_fisheries", "transport"  ] \

echo "Installation Complete"

