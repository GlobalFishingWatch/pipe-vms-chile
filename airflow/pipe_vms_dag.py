from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

from datetime import datetime

import imp
import posixpath as pp


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
pipe_events_anchorages = imp.load_source('pipe_events_anchorages', get_dag_path('pipe_events.anchorages','pipe_events_anchorages'))
pipe_events_encounters = imp.load_source('pipe_events_encounters', get_dag_path('pipe_events.encounters','pipe_events_encounters'))
pipe_events_fishing = imp.load_source('pipe_events_fishing', get_dag_path('pipe_events.fishing','pipe_events_fishing'))
pipe_events_gaps = imp.load_source('pipe_events_gaps', get_dag_path('pipe_events.gaps','pipe_events_gaps'))


date_branches = [
    (None                 ,  datetime(2019, 5, 22), 'historic'),
    (datetime(2019, 5, 23),  None,                  'naf_daily')
]


def table_partition_check(name, dataset_id, table_id, date, fleet):
    return BigQueryCheckOperator(
        task_id='table_partition_check_{}_{}'.format(fleet, name),
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
        mode='reschedule',               # the sensor task frees the worker slot when the criteria is not yet met
                                         # and it's rescheduled at a later time.
        poke_interval=10 * 60,           # check every 10 minutes.
        timeout=60 * 60 * 24,            # timeout of 24 hours.
        retry_exponential_backoff=False, # disable progressive longer waits
        retries=0,                       # no retries and lets fail the task
        on_failure_callback=config_tools.failure_callback_gfw
    )


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
        fleets = Variable.get(PIPELINE, deserialize_json=True)['fleets']

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_exists = []
            source_naf_exists = []

            date_branch = ExecutionDateBranchOperator(
                task_id='date_branch',
                date_branches=date_branches
            )
            naf_daily = DummyOperator(task_id='naf_daily')
            historic = DummyOperator(task_id='historic')

            for fleet in fleets:
                source_exists.append(table_partition_check(
                    'historic',
                    '{source_dataset}'.format(**config),
                    '{source_table}'.format(**config),
                    '{ds}'.format(**config),
                    '{}'.format(fleet)))

            fetch_normalized_historic = self.build_docker_task({
                'task_id':'fetch_normalized_historic',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'fetch-normalized-historic',
                'dag':dag,
                'arguments':['fetch_normalized_vms',
                             '{source_dates}'.format(**config),
                             'historic',
                             '{source_paths}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{normalized}'.format(**config)]
            })

            #---- NAF------
            for fleet in fleets:
                source_naf_exists.append(table_partition_check(
                    'naf_daily',
                    '{source_naf_dataset}'.format(**config),
                    '{source_naf_table}'.format(**config),
                    '{ds}'.format(**config),
                    '{}'.format(fleet)))

            fetch_normalized_naf_daily = self.build_docker_task({
                'task_id':'fetch_normalized_naf_daily',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'fetch-normalized-naf-daily',
                'dag':dag,
                'arguments':['fetch_normalized_vms',
                             '{source_dates}'.format(**config),
                             'naf_daily',
                             '{project_id}:{source_naf_dataset}.{source_naf_table}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{normalized}'.format(**config)]
            })
            #---- NAF------


            segment = SubDagOperator(
                subdag=pipe_segment.PipeSegmentDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=dict(
                        pipeline_dataset=config['pipeline_dataset'],
                        source_dataset=config['pipeline_dataset'],
                        source_tables='{normalized}'.format(**config),
                        dataflow_runner='{dataflow_runner}'.format(**config),
                        temp_shards_per_day="3",
                    )
                ).build(dag_id='{}.segment'.format(dag_id)),
                trigger_rule=TriggerRule.ONE_SUCCESS,
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
                subdag=pipe_anchorages.PipeAnchoragesPortEventsDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.port_events'.format(dag_id)),
                task_id='port_events'
            )

            port_visits = SubDagOperator(
                subdag=pipe_anchorages.PipeAnchoragesPortVisitsDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.port_visits'.format(dag_id)),
                task_id='port_visits'
            )

            encounters = SubDagOperator(
                subdag=pipe_encounters.PipeEncountersDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.encounters'.format(dag_id)),
                task_id='encounters'
            )


            dag >> date_branch >> historic
            for existence_sensor in source_exists:
                historic >> existence_sensor >> fetch_normalized_historic

            dag >> date_branch >> naf_daily
            for existence_sensor in source_naf_exists:
                naf_daily >> existence_sensor >> fetch_normalized_naf_daily

            fetch_normalized_historic >> segment
            fetch_normalized_naf_daily >> segment

            segment >> measures

            measures >> port_events >> port_visits
            measures >> encounters

            if config.get('enable_features_events', False):

                features = SubDagOperator(
                    subdag=pipe_features.PipeFeaturesDagFactory(
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.features'.format(dag_id)),
                    depends_on_past=True,
                    task_id='features'
                )

                events_anchorages = SubDagOperator(
                    subdag = pipe_events_anchorages.PipelineDagFactory(
                        config_tools.load_config('pipe_events.anchorages'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_anchorages'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_anchorages'
                )

                events_encounters = SubDagOperator(
                    subdag = pipe_events_encounters.PipelineDagFactory(
                        config_tools.load_config('pipe_events.encounters'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_encounters'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_encounters'
                )

                events_fishing = SubDagOperator(
                    subdag = pipe_events_fishing.PipelineDagFactory(
                        config_tools.load_config('pipe_events.fishing'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_fishing'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_fishing'
                )

                events_gaps = SubDagOperator(
                    subdag = pipe_events_gaps.PipelineDagFactory(
                        config_tools.load_config('pipe_events.gaps'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_gaps'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_gaps'
                )

                port_visits >> features
                encounters >> features

                # Points to each independent event
                features >> events_anchorages
                features >> events_encounters
                features >> events_fishing
                features >> events_gaps

        return dag


for mode in ['daily','monthly', 'yearly']:
    dag_id = '{}_{}'.format(PIPELINE, mode)
    globals()[dag_id] = PipelineDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)
