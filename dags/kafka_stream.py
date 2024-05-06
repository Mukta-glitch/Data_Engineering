from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner' :'airscholar',
    'start_date': datetime(2023,9,3,10,00)
}

def get_data():
    import requests
    res=requests.get("https://randomuser.me/api/")
    res=res.json()
    res=res['results'][0]
    return res

def format_data(res):
    location = res["location"]
    data = {
        "gender": res["gender"],
        "title": res["name"]["title"],
        "first_name": res["name"]["first"],
        "last_name": res["name"]["last"],
        "postcode": location["postcode"],
        "latitude": location["coordinates"]["latitude"],
        "longitude": location["coordinates"]["longitude"],
        "timezone_offset": location["timezone"]["offset"],
        "timezone_description": location["timezone"]["description"],
        "email": res["email"],
        "username": res["login"]["username"],
        "password": res["login"]["password"],
        "dob_date": res["dob"]["date"],
        "dob_age": res["dob"]["age"],
        "registered_date": res["registered"]["date"],
        "registered_age": res["registered"]["age"],
        "phone": res["phone"],
        "cell": res["cell"],
        "id_name": res["id"]["name"],
        "id_value": res["id"]["value"],
        "picture_large": res["picture"]["large"],
        "picture_medium": res["picture"]["medium"],
        "picture_thumbnail": res["picture"]["thumbnail"],
        "nat": res["nat"]
    }

    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"

    return data

def stream_data():
    import json
    import requests
    res=get_data()
    res=format_data(res)
    print(json.dumps(res,indent=3))

# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False)as dag:
    
#     streaming_task =PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data()
