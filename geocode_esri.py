import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json

database_parameters = {
    'host': 'HOST ADDRESS',
    'port': 'PORT',
    'user': 'USER NAME',
    'password': 'PASSWORD',
    'dbname': 'DATABASE NAME'
}

access_token = "API TOKEN HERE"

conn = psycopg2.connect(**database_parameters)
conn.autocommit = True
cursor = conn.cursor(cursor_factory=RealDictCursor)

def get_latitude_longitude(address, access_token):
    url = "https://geocode-api.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
    params = {
        "f": "pjson",
        "singleLine": address,
        "token": access_token
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 

        data = response.json()
        if data["candidates"]:
            candidate = data["candidates"][0]
            return {"latitude": candidate["location"]["y"], "longitude": candidate["location"]["x"]}
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error making request to Esri geocoding API: {e}")
        return None
    
#CHANGE YOUR TABLE NAME
#APPLY LIMIT IN SQL IF YOU DO NOT WANT TO CROSS THE FREE TIER LIMIT
cursor.execute("SELECT id, address from dwasa_call_center where geom is null")
addresses = cursor.fetchall()


if addresses:
    i = 0
    for address in addresses:
        i = i + 1
        print(f"Count: {i}")

        id = address['id']
        problem_location = address['address']        
        lat_lng = get_latitude_longitude(problem_location, access_token)

        if lat_lng:
            geojson = {
                "type": "Point",
                "coordinates":[lat_lng['longitude'], lat_lng['latitude']]
            }
            geojson_str = json.dumps(geojson)

            #CHANGE YOUR TABLE NAME ACCORDINGLY
            query = f'''
                Update dwasa_call_center set geom = ST_SetSRID(ST_GeomFromGeoJSON('{geojson_str}'),4326) where id = {id} 
            '''
            cursor.execute(query)
            print(f"Coordinates for {problem_location}: {lat_lng}")
            print("---------------------")
        else:
            print(f"Failed to geocode: {problem_location}")

