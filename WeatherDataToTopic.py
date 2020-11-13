from kafka import KafkaProducer
import time
import requests
import json


kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_name = 'sample_topic'

producer = KafkaProducer(bootstrap_servers = kafka_bootstrap_servers,
value_serializer = lambda v: json.dumps(v).encode('utf-8'))

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = None

def get_weather_detail(openweathermap_api_endpoint):
    print(openweathermap_api_endpoint)
    api_response = requests.get(openweathermap_api_endpoint)
    json_data = api_response.json()
    city_name = json_data['name']
    humidity = json_data['main']['humidity']
    temperature = json_data['main']['temp']
    json_message = {'CityName': city_name,
    "temperature": temperature,
    "Humidity": humidity,
    "CreationTime": time.strftime("%y-%m-%d %H:%M:%S")}
    return json_message


def get_apikey():
    with open('weatherapikey.json') as f:
        return json.load(f)['weatherdetail']

# a = requests.get("http://api.openweathermap.org/data/2.5/weather?q=Chennai&appid=98a1502877eff8b08da85801cf53cdc5")
# b = a.json()
# print(b['name'])


while True:
    city_name = 'Hyderabad'
    api_key = get_apikey()
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q=" + city_name + "&appid=" + api_key
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print(json_message)
    time.sleep(2)


    city_name = 'Chennai'
    api_key = get_apikey()
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q=" + city_name + "&appid=" + api_key
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print(json_message)
    time.sleep(2)


    city_name = 'Mumbai'
    api_key = get_apikey()
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q=" + city_name + "&appid=" + api_key
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print(json_message)
    time.sleep(2)



    city_name = 'Bangalore'
    api_key = get_apikey()
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q=" + city_name + "&appid=" + api_key
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print(json_message)
    time.sleep(2)




