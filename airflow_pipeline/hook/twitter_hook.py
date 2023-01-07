from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime, timedelta
import json


class TwitterHook(HttpHook):

    def __init__(self, start_time, end_time, query, conn_id=None):
        
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time
        self.query = query

        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        # URL
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        start_time = self.start_time
        end_time = self.end_time
        query = self.query

        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw
    
    def connect_to_endpoint(self, url, session):
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {url}")

        return self.run_and_check(session, prep, {})
    
    def paginate(self, url_raw, session):

        # imprimir json
        lista_json_response = []
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()

        lista_json_response.append(json_response)

        contador = 1
        # paginacao
        while "next_token" in json_response.get("meta", {}) and contador < 100:
            next_token = json_response['meta']['next_token']
            url = f'{url_raw}&next_token={next_token}'
            response = self.connect_to_endpoint(url, session)
            json_response = response.json()

            lista_json_response.append(json_response)
            contador += 1

        return lista_json_response
    
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()

        return self.paginate(url_raw, session)

if __name__ == "__main__":
    # URL
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)

    query = "airflow"

    for page in TwitterHook(start_time, end_time, query).run():
        print(json.dumps(page, indent=4, sort_keys=True))