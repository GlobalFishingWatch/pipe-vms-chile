import posixpath as pp
import imp
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_ext.gfw.models import DagFactory


PIPELINE = 'pipe_vms_chile'

def get_dag_path(pipeline, module=None):
    if module is None:
        module = pipeline
    config = Variable.get(pipeline, deserialize_json=True)
    return pp.join(config['dag_install_path'], '{}_dag.py'.format(module))


pipe_segment = imp.load_source('pipe_segment', get_dag_path('pipe_segment'))
pipe_measures = imp.load_source('pipe_measures', get_dag_path('pipe_measures'))
pipe_anchorages = imp.load_source('pipe_anchorages', get_dag_path('pipe_anchorages'))
pipe_encounters = imp.load_source('pipe_encounters', get_dag_path('pipe_encounters'))
pipe_features = imp.load_source('pipe_features', get_dag_path('pipe_features'))
pipe_events = imp.load_source('pipe_events', get_dag_path('pipe_events'))


#
# PIPE_VMS_chile
#
class PipelineDagFactory(DagFactory):
    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipelineDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        config = self.config
        default_args = self.default_args
        subdag_default_args = dict(
            start_date=default_args['start_date'],
            end_date=default_args['end_date']
        )
        subdag_config = dict(
            pipeline_dataset=config['pipeline_dataset'],
            source_dataset=config['pipeline_dataset'],
            events_dataset=config['events_dataset'],
            dataflow_runner='{dataflow_runner}'.format(**config),
            temp_shards_per_day="3",
        )
        config['source_paths'] = ','.join(self.source_table_paths())
        config['source_dates'] = ','.join(self.source_date_range())

        
        def table_partition_check(dataset_id, table_id, date):
            return BigQueryCheckOperator(
                task_id='table_partition_check',
                dataset_id=dataset_id,
                sql='SELECT COUNT(*) FROM [{}.{}${}]'.format(dataset_id, table_id, date),
                retries=24*2*7,       # retry twice per hour for a week
                retry_delay=timedelta(minutes=30),
                retry_exponential_backoff=False
            )

        
        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:            
            source_exists=table_partition_check(
                '{source_dataset}'.format(**config),
                '{source_table}'.format(**config),
                '{ds_nodash}'.format(**config))
                        
            
            fetch_normalized = BashOperator(
                task_id='fetch_normalized_daily',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} fetch_normalized_vms '
                             '{source_dates} '
                             '{source_paths} '
                             '{project_id}:{pipeline_dataset}.{normalized} '
                             ''.format(**config)
            )

            segment = SubDagOperator(
                subdag=pipe_segment.PipeSegmentDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=dict(
                        pipeline_dataset=config['pipeline_dataset'],
                        source_dataset=config['pipeline_dataset'],
                        normalized_tables='{normalized}'.format(**config),
                        dataflow_runner='{dataflow_runner}'.format(**config),
                        temp_shards_per_day="3",
                    )
                ).build(dag_id='{}.segment'.format(dag_id)),
                depends_on_past=True,
                task_id='segment'
            )

            measures = SubDagOperator(
                subdag=pipe_measures.PipeMeasuresDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.measures'.format(dag_id)),
                task_id='measures'
            )

            port_events = SubDagOperator(
                subdag=pipe_anchorages.build_port_events_dag(
                    dag_id='{}.port_events'.format(dag_id),
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ),
                task_id='port_events'
            )

            port_visits = SubDagOperator(
                subdag=pipe_anchorages.build_port_visits_dag(
                    dag_id='{}.port_visits'.format(dag_id),
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ),
                task_id='port_visits'
            )

            encounters = SubDagOperator(
                subdag=pipe_encounters.build_dag(
                    dag_id='{}.encounters'.format(dag_id),
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ),
                task_id='encounters'
            )

            features = SubDagOperator(
                subdag=pipe_features.PipeFeaturesDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.features'.format(dag_id)),
                depends_on_past=True,
                task_id='features'
            )

            events = SubDagOperator(
                subdag=pipe_events.PipelineDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.events'.format(dag_id)),
                depends_on_past=True,
                task_id='events'
            )

            dag >> source_exists >> fetch_normalized

            fetch_normalized >> segment >> measures

            measures >> port_events >> port_visits >> features
            measures >> encounters >> features
            features >> events

        return dag


pipe_vms_daily_dag = PipelineDagFactory().build(dag_id='{}_daily'.format(PIPELINE))
pipe_vms_monthly_dag = PipelineDagFactory(schedule_interval='@monthly').build(dag_id='{}_monthly'.format(PIPELINE))
pipe_vms_yearly_dag = PipelineDagFactory(schedule_interval='@yearly').build(dag_id='{}_yearly'.format(PIPELINE))