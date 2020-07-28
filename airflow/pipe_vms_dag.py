from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.models import DAG
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

from datetime import timedelta


PIPELINE = 'pipe_vms_chile'

def table_partition_check(dataset_id, table_id, date, fleet):
    """
    Checks the BigQuery table of a specific partition.

    :@param dataset_id: The id of BQ dataset.
    :@type dataset_id: str.
    :@param table_id: The id of BQ table.
    :@type table_id: str.
    :@param date: The date of the partition.
    :@type date: str.
    :@param fleet: The fleet to be checked.
    :@type fleet: str.
    :@return: A BigQueryCheckOperatior checking the partition table.
    """
    return BigQueryCheckOperator(
        task_id='table_partition_check_{}'.format(fleet),
        use_legacy_sql=False,
        dataset_id=dataset_id,
        sql='SELECT '
                'COUNT(*) FROM `{dataset}.{table}` '
            'WHERE '
                'timestamp > Timestamp("{date}") '
                'AND timestamp <= TIMESTAMP_ADD(Timestamp("{date}"), INTERVAL 1 DAY) '
                'AND fleet = "{fleet}"'
            .format(
                dataset=dataset_id,
                table=table_id,
                date=date,
                fleet=fleet),
        retries=2*24*2,                        # Retries 2 days with 30 minutes.
        execution_timeout=timedelta(days=2),   # TimeOut of 2 days.
        retry_delay=timedelta(minutes=30),     # Delay in retries 30 minutes.
        max_retry_delay=timedelta(minutes=30), # Max Delay in retries 30 minutes
        on_failure_callback=config_tools.failure_callback_gfw
    )

#
# PIPE_VMS_chile
#
class PipelineDagFactory(DagFactory):
    """Concrete class to handle the DAG for pipe_vms_chile."""

    def __init__(self, pipeline=PIPELINE, **kwargs):
        """
        Constructs the DAG.

        :@param pipeline: The pipeline name. Default value the PIPELINE.
        :@type pipeline: str.
        :@param kwargs: A dict of optional parameters.
        :@param kwargs: dict.
        """
        super(PipelineDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        """
        Override of build method.

        :@param dag_id: The id of the DAG.
        :@type table: str.
        """
        config = self.config
        config['source_paths'] = ','.join(self.source_table_paths())
        config['source_dates'] = ','.join(self.source_date_range())
        fleets = Variable.get(PIPELINE, deserialize_json=True)['fleets']

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_exists = []

            for fleet in fleets:
                source_exists.append(table_partition_check(
                    '{source_dataset}'.format(**config),
                    '{source_table}'.format(**config),
                    '{ds}'.format(**config),
                    '{}'.format(fleet)))

            fetch_normalized = self.build_docker_task({
                'task_id':'fetch_normalized_daily',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'fetch-normalized-daily',
                'dag':dag,
                'arguments':['fetch_normalized_vms',
                             '{source_dates}'.format(**config),
                             '{source_paths}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{normalized}'.format(**config)]
            })

            for existence_sensor in source_exists:
                dag >> existence_sensor >> fetch_normalized

        return dag


for mode in ['daily','monthly', 'yearly']:
    dag_id = '{}_{}'.format(PIPELINE, mode)
    globals()[dag_id] = PipelineDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)
