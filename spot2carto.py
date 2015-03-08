''' spot2carto.py
    
    Retrieves new Spot PCB data from public API and writes to CartoDB

    ***Must store Spot and CartoDB API key in json file named 'keys.json'***
'''
import json, requests

with open('keys.json', 'r') as f:
    keys = json.load(f)

SPOT_KEY = keys.get('SPOT')
SPOT_URL = 'https://api.findmespot.com/spot-main-web/consumer/rest-api/2.0/public/feed/{}/message.json'.format(SPOT_KEY)

CARTODB_KEY = keys.get('CARTODB')
CARTODB_USER = 'bwainstock'
CARTODB_URL = 'http://{user}.cartodb.com/api/v2/sql?q={sql}&api_key={key}'


def get_spot_json():
    response = requests.get(SPOT_URL)
    return response.json()['response']['feedMessageResponse']['messages']['message']


def cartodb_write(json_data, table):
    maxtime = cartodb_latest(table)
    if not maxtime:
        maxtime = 0
    for point in json_data:
        print(point)
        modelid = point['modelId']
        messagetype = point['messageType']
        messengerid = point['messengerId']
        userid = point['id']
        latitude = point['latitude']
        longitude = point['longitude']
        unixtime = point['unixTime']
        datetime = point['dateTime']
        the_geom = "ST_GeomFromText('POINT({} {})', 4326)".format(longitude, latitude)
        columns = 'modelid, message_type, messengerid, id, latitude, longitude, unixtime, datetime, the_geom'

        if unixtime > maxtime:
            query = "INSERT INTO {} ({}) VALUES ('{}','{}','{}','{}',{},{},{},to_timestamp('{}', 'YYYY-MM-DD HH24:MI:SS'),{})".format(
                table, columns, modelid, messagetype, messengerid, userid,
                latitude, longitude, unixtime, datetime, the_geom)
            response = requests.get(CARTODB_URL.format(user=CARTODB_USER, sql=query, key=CARTODB_KEY))
            if response.status_code != 200:
                print("Error: {}".format(response.content))
            else:
                print('New lat/lon found! {}'.format(datetime))


def cartodb_latest(table):
    query = 'SELECT MAX(unixtime) FROM {}'.format(table)
    response = requests.get(CARTODB_URL.format(user=CARTODB_USER, sql=query, key=CARTODB_KEY))
    return response.json()['rows'][0]['max']


def main():
    table_name = 'test'
    data = get_spot_json()
    cartodb_write(data, table_name)

if __name__ == '__main__':
    main()
