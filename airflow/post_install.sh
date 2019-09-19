#!/bin/bash

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_vms_chile \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    events_dataset="pipe_chile_production_v20190509" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="pipe-chile-production-v20190509" \
    pipeline_dataset="pipe_chile_production_v20190509" \
    source_dataset="VMS_Chile" \
    source_table="raw_all_view" \
    normalized="chile_vms_normalized_" \
    fleets="" \

echo "Installation Complete"

