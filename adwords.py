"""
### Dag Documentation
This dag extracts data from AdWords API and loads it into data lake (S3)
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks import SSHHook
import os
from datetime import datetime, timedelta
from googleads import adwords
import logging
import suds
import time
import json
from tempfile import NamedTemporaryFile
from string import Template


logger = logging.getLogger()
logger.setLevel(logging.INFO)
configs = json.load(open(os.path.join(os.path.dirname(__file__), 'adwords_config.json')))  # the config file should be within the dag folder
ENV = Variable.get('env')


ACCOUNTS = configs["ACCOUNTS"]  # The logic of getting accounts from api still need to be tested. so keep it as-is now.
FILE_TYPE = configs["FILE_TYPE"]
REPORT_TYPES = configs["REPORT_TYPES"]
RETRYABLE_ERRORS = configs["RETRYABLE_ERRORS"]
MAX_RETRY = configs["MAX_RETRY"]
RETRY_INTERVAL = configs["RETRY_INTERVAL"]
BUCKET_NAME = configs[ENV]["BUCKET_NAME"]
SSH_HOOK = configs[ENV]["SSH_HOOK"]
HIVE_SCRIPT = configs[ENV]["HIVE_SCRIPT"]
S3_PREFIX = configs[ENV]["S3_PREFIX"]
S3_CONN_ID = configs[ENV]["S3_CONN_ID"]
HIVE_ROLE = configs[ENV]["HIVE_ROLE"]
RAW_PATH = configs[ENV]["RAW_PATH"]
PROD_PATH = configs[ENV]["PROD_PATH"]



# run partial backfill or full backfill based on env
def get_start_date():
    # return different start date based on environment, for dev we dont need to do full backfill
    if Variable.get('env') == 'prod':
        return datetime(2016, 1, 1, 16)
    
    return datetime(2017, 2, 1, 16)


# Default args that could be inherited by each task of your dag
default_args = {
    'owner': 'bgmop',
    'depends_on_past': False,
    'start_date': get_start_date(),
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}


# Instantiate your dag by specifying your dag name and args (default and non-default)
dag = DAG('adtech_adwords_report_daily',  # dag id will be modified when in airflow-dev
          default_args=default_args,
          max_active_runs=4,
          concurrency=4,
          schedule_interval=timedelta(days=1)
          )


# Define your python_callable
def get_report(ds, **kwargs):
    # use airflow variable, leverage backfill if server was down 
    logger.info('start getting report for {0}'.format(ds))

    # authentication from a YAML string
    # build it form variables
    cred = "adwords: \n"
    cred += "  developer_token: {0}\n".format(Variable.get('adwords_developer_token'))
    cred += "  client_id: {0}\n".format(Variable.get('adwords_client_id'))
    cred += "  client_secret: {0}\n".format(Variable.get('adwords_client_secret'))
    cred += "  refresh_token: {0}".format(Variable.get('adwords_refresh_token'))

    # api authentication
    aw_client = adwords.AdWordsClient.LoadFromString(cred)
    
    # for each accuont_id, get reports 
    for client_customer_id in ACCOUNTS:
        aw_client.SetClientCustomerId(client_customer_id)
        for item in REPORT_TYPES.items():
            get_report_daily_account_type(aw_client, item, ds)


def get_report_daily_account_type(client, report, data_date):
    logger.info('getting {0} for account {1}'.format(report[0], client.client_customer_id))

    # get report downloader service
    report_downloader = client.GetReportDownloader(version='v201609')

    ''' 
        Try making API calls, handle some API retryable errors suggested by their documentation. 
        retry for MAX_RETRY times, if still failing, raise that error
        For other errors, re-raise it
    '''
    retry_attempt = 1
    while True:
        try:
            # the api call to get the data in cache
            temp = NamedTemporaryFile()

            # Config has yesterday as default, but should use {{ds}} for some corner cases
            report[1]['dateRangeType'] = 'CUSTOM_DATE'
            report[1]['selector']['dateRange'] = {'max': data_date, 'min': data_date}

            data = report_downloader.DownloadReport(
                report[1], temp, skip_report_header=True, skip_column_header=False,
                skip_report_summary=True, include_zero_impressions=False
            )
            temp.flush()

            # log file size
            logger.info('file size: {0} MB'.format(os.path.getsize(temp.name) >> 20))  # log size in MB

            ymd = data_date.split('-')  # split the date string YYYY-mm-dd
            # upload it directly to s3
            s3_conn = S3Hook(s3_conn_id=S3_CONN_ID)  # this will be the connection id in airflow ui
            key = '{5}{0}/year={1}/month={2}/day={3}/{4}'.format(
                report[0], ymd[0], ymd[1], ymd[2], client.client_customer_id, S3_PREFIX
            )
            
            # by default, function returns logging at INFO level
            s3_conn.load_file(filename=temp.name, key=key, bucket_name=BUCKET_NAME, replace=True) 

            # close the temp file, therefore deleting it
            temp.close()
            
            logger.info('call succeeded')
            break
        except suds.WebFault, e:
            if retry_attempt <= MAX_RETRY:
                for error in e.fault.detail.ApiExceptionFault.errors:
                    if error['ApiError.Type'] in RETRYABLE_ERRORS:
                        logger.error(error)
                        # using exponential backoff intervals to retry
                        time.sleep(RETRY_INTERVAL * (2 ** retry_attempt))
                        logger.info('Start retry attempt {}'.format(retry_attempt))
                        # increment count for next attempt
                        retry_attempt += 1
                    else:
                        # re-throw exception if it's not a retryable error
                        raise e
            else:
                logger.error('Still receiving errors after {0} retries. Aborting...'.format(MAX_RETRY))
                raise e


t1 = PythonOperator(task_id='get_report',
                    python_callable=get_report,
                    provide_context=True,
                    dag=dag)


# parse the hive command in template form, leverages airflow macro variables
def parse_hive_command(report_name):
    # use bash function to split {{ ds }}, evaluated in YYYY-MM-DD
    # $$ used to escape being evaluated as variable substitution in template.susbstitute
    raw_table_name = 'adwords_{0}_raw'.format(report_name.lower())
    table_name = 'adwords_{0}'.format(report_name.lower())
    if ENV == 'dev':
        raw_table_name = 'dev_' + raw_table_name
        table_name = 'dev_' + table_name

    return Template('IFS="-" read -r -a ymd <<< {{ ds }}; '  
                    '/usr/bin/hive -f $hive_script '
                    '--hivevar yr="$${ymd[0]}" --hivevar mth="$${ymd[1]}" --hivevar dt="$${ymd[2]}" '
                    '--hivevar role=$role --hivevar raw_path=$raw_path --hivevar prod_path=$prod_path '
                    '--hivevar raw_table=$raw_table --hivevar table=$table'
                    ).substitute(hive_script=HIVE_SCRIPT.replace('hive_daily', 'hive_{0}_daily'.format(report_name.lower())),
                                 role=HIVE_ROLE, 
                                 raw_path=RAW_PATH + report_name + '/', 
                                 prod_path=PROD_PATH + report_name + '/',
                                 raw_table=raw_table_name,
                                 table=table_name)


# divide task 2 into parallel running subdags for better workflow monitoring, management. 
def create_sub_dag(parent_dag, report_name):
    sub_dag = DAG(dag_id=parent_dag.dag_id + '.hive_' + report_name, default_args=parent_dag.default_args)

    # Use ssh operator that executes a hive script in our etl always on cluster
    hive_task = SSHExecuteOperator(task_id='hive_transformation',
                            ssh_hook=SSHHook(SSH_HOOK),
                            bash_command=parse_hive_command(report_name),
                            dag=sub_dag)

    return SubDagOperator(task_id='hive_' + report_name,
                          subdag=sub_dag,
                          default_args=parent_dag.default_args,
                          dag=parent_dag)


# TODO: task 3. test task that validates the end data.
t3 = DummyOperator(task_id='placeholder_for_validation_task',
                   dag=dag)


# connect tasks by adding dependency.
for rn in REPORT_TYPES.keys():
    t2 = create_sub_dag(dag, rn)
    t2.set_upstream(t1)
    t2.set_downstream(t3)
