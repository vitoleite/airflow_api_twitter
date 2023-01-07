import sys
sys.path.append("/home/vitor/airflow_alura_twitter")

from airflow.models import BaseOperator, DAG, TaskInstance
from airflow_pipeline.hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path


class TwitterOperator(BaseOperator):

    template_fields = ["file_path", "start_time", "end_time", "query"]

    def __init__(self, file_path, start_time, end_time, query, **kwargs):
        self.file_path = file_path
        self.start_time=start_time
        self.end_time=end_time
        self.query=query

        super().__init__(**kwargs)
    
    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self, context):

        start_time=self.start_time
        end_time=self.end_time
        query=self.query

        self.create_parent_folder()

        with open(self.file_path, "w") as output:
            for page in TwitterHook(start_time, end_time, query).run():
                json.dump(page, output, ensure_ascii=False)
                output.write("\n")


if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    query = "airflow"

    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            file_path=join(
                "datalake/twitter_airflow",
                f"extract_date={datetime.now().date()}",
                f"airflow_{datetime.now().date().strftime('%Y%m%d')}.json"
            ),
            start_time=start_time, end_time=end_time, query=query, task_id="test_run"
        )
        ti = TaskInstance(task=to)

        to.execute(ti.task_id)