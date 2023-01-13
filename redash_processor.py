# 리대시 처리용 코드
import requests
from redashAPI import RedashAPIClient
import time
import json
import pandas as pd


REDASH_BASE_URL = 'https://redash.buzzvil.com/'
TOY_REDASH_API_KEY = 'vjmZYfRNTZ9vh4MIMaCm9SmO1ydYf0GRmOl9McvQ'



def get_fresh_query_result_in_df(redash_url, api_key, query_id, params):
    def poll_job(s, redash_url, job):
        # TODO: add timeout
        while job['status'] not in (3,4):
            response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
            job = response.json()['job']
            time.sleep(1)

        if job['status'] == 3:
            return job['query_result_id']
        return None
    # 1) API로 결과 json 형태로 받아오기
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
    payload = dict(max_age=0, parameters=params)
    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))
    # response = s.get('https://redash.buzzvil.com/api/queries/11592/results.json?api_key=EnAP7yXds4aOCvbPMsSUswcifhTwbt6dwVZgd69Z')
    if response.status_code != 200:
        print(response.status_code) 
        print(response.json()) 
        raise Exception('Refresh failed.')
    result_id = poll_job(s, redash_url, response.json()['job'])
    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')
    # 2) json -> df 으로 형변환
    result_in_json =  response.json()['query_result']['data']['rows']
    result_in_df = pd.DataFrame(result_in_json)
    return result_in_df
        

# 1) 리대시 쿼리를 통해 버즈빌 데이터를 가져오는 함수 -> 기간, 라인아이템 ID
def get_conversion_df_of_a_lineitem(first_date,last_date , lineitem_id):
    p = {'lineitem_id': lineitem_id,'first_date':first_date ,'last_date': last_date}
    return get_fresh_query_result_in_df(
        redash_url=REDASH_BASE_URL,
        api_key=TOY_REDASH_API_KEY,
        query_id='11665',
        params=p)
    

if __name__ == '__main__':
    df = get_conversion_df_of_a_lineitem(
        lineitem_id='1979408',
        first_date='2022-12-16',
        last_date='2022-12-17'
    )
    print(df)