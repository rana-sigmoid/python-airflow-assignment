import os
import requests
import pandas as pd


def get_weather_api_method():
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    # 10 States
    state_list = ['Assam', 'Haryana', 'Gujarat', 'Maharashtra', 'karnataka', 'punjab', 'Rajasthan', 'Bihar', 'West Bengal','Odisha']
    df = pd.DataFrame(columns=["State", "Description", "Temperature", "Feels_Like_Temperature", "Min_Temperature", "Max_Temperature",
                 "Humidity", "Clouds"])
    for state in state_list:
        querystring = {"q": state}
        # API Host and Key
        headers = {
            'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
            'x-rapidapi-key': "40b27a5d62mshead33450c6e50ccp159aeejsn1841eea50cff"
        }

        response = requests.get(url, headers=headers, params=querystring)
        info = response.json()
        # time.sleep(10)
        try:
            df = df.append({'State': info['name'], "Description": info['weather'][0]['description'],
                            'Temperature': info['main']['temp'], "Feels_Like_Temperature": info['main']['feels_like'],
                            "Min_Temperature": info['main']['temp_min'], "Max_Temperature": info['main']['temp_max'],
                            "Humidity": info['main']['humidity'], "Clouds": info['clouds']['all']}, ignore_index=True)
        except:
            print("API Request limit exceeds")

    path = "/usr/local/airflow/store_files_airflow"
    if not os.path.isfile(os.path.join(path, '/weather_data.csv')):
        df.to_csv(path + '/weather_data.csv', index=False)
    else:
        os.remove(os.path.join(path, '/weather_data.csv'))
        df.to_csv(os.path.join(path, '/weather_data.csv'), index=False)
    print(df.head())
