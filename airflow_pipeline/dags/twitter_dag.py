import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from os.path import join
from airflow.utils.dates import days_ago


with DAG(dag_id="TwitterDAG", start_date=days_ago(2), schedule_interval="@daily") as dag:

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    query = "pyspark"

    to = TwitterOperator(
        file_path=join(
            f"datalake/twitter_{query}",
            "extract_date={{ ds }}",
            f"{query}_{{{{ ds_nodash }}}}.json"
        ),
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        query=query,
        task_id=f"twitter_{query}"
    )