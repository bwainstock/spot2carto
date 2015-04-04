""" spot2carto.py
    
    Retrieves new Spot PCB data from public API and writes to CartoDB

    ***Must store Spot and CartoDB API key in json file named 'keys.json'***
"""
import json
import requests

with open('keys.json', 'r') as f:
    keys = json.load(f)

SPOT_KEYS = keys.get('SPOT')
SPOT_URL = 'https://api.findmespot.com/spot-main-web/consumer/rest-api/2.0/public/feed/{}/message.json'

CARTODB_KEY = keys.get('CARTODB')
CARTODB_USER = 'bwainstock'
CARTODB_URL = 'http://{user}.cartodb.com/api/v2/sql?q={sql}&api_key={key}'


def get_cartodb(query):
    """
    Sends HTTP GET request to CartoDB SQL API with 'query'
    Parameters
    ----------
    query - string SQL query

    Returns:
    --------
    response - requests Response object
    """
    response = requests.get(CARTODB_URL.format(user=CARTODB_USER, sql=query, key=CARTODB_KEY))
    return response


def get_spot_json():
    """
    Retrieves SPOT API JSON feed for given feed IDs in keys.json

    Returns:
    --------

    responses - list of JSON objects with most recent SPOT checkins
    """
    responses = []
    for key in SPOT_KEYS.values():
        response = requests.get(SPOT_URL.format(key))
        json_response = response.json()
        if json_response['response'].get('feedMessageResponse'):
            responses.append((key, json_response['response']['feedMessageResponse']['messages']['message']))
        elif json_response['response'].get('errors'):
            print(json_response['response']['errors']['error']['description'])
        else:
            print('Response problem.')

    return responses


def table_exists(table):
    query = 'select * from {}'.format(table)
    response = get_cartodb(query)
    json_response = response.json()
    if json_response.get('fields'):
        return True
    elif json_response.get('error'):
        if 'does not exist' in str(json_response['error']):
            return False
        return response
    return response


def instantiate_cartodb_table(table):
    """
    CartoDB editor needs a special schema for the table (the_geom, cartodb_id).

    Parameters:
    ----------
    table - table name to instantiate

    Returns:
    -------
    SQL response from CartoDB API
    """
    query = "select cdb_cartodbfytable('{}');".format(table)
    response = get_cartodb(query)
    # if response.json().get('time'):
    return response


def create_line_table(table='lines'):
    """
    :return:
    """
    new_columns = 'spot_key text UNIQUE'
    query = 'CREATE TABLE {} ({});'.format(table, new_columns)
    response = get_cartodb(query)
    if response.json().get('time'):
        response = instantiate_cartodb_table(table)
    return response


def cartodb_write(json_responses, table):
    for key, json_data in json_responses:
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
            columns = 'feedid, modelid, message_type, messengerid, id, latitude, longitude, unixtime, datetime, the_geom'

            if unixtime > maxtime:
                query = "INSERT INTO {} ({}) VALUES ('{}','{}','{}','{}','{}',{},{},{},to_timestamp('{}', 'YYYY-MM-DD HH24:MI:SS'),{})".format(
                    table, columns, key, modelid, messagetype, messengerid, userid,
                    latitude, longitude, unixtime, datetime, the_geom)
                response = get_cartodb(query)
                print(query)
                if response.status_code != 200:
                    print("Error: {}".format(response.content))
                else:
                    print('New lat/lon found! {}'.format(datetime))


def cartodb_line(json_responses, line_table, table):
    for key, _ in json_responses:
        # query = "UPDATE {} SET the_geom = (SELECT ST_MakeLine(the_geom) from {} WHERE feedid='{}' ORDER BY unixtime DESC) WHERE feedid='{}'"
        subquery = "SELECT ST_MakeLine(the_geom) FROM (SELECT * FROM {} ORDER BY unixtime DESC) AS tempTable WHERE feedid='{}'".format(table, key)
        query = "UPDATE {} SET the_geom = ({}) WHERE feedid='{}'".format(line_table, subquery, key)
        response = get_cartodb(query.format(line_table, table, key, key))
        print(response.json())
        print('Line updated')

def cartodb_latest(table):
    query = 'SELECT MAX(unixtime) FROM {}'.format(table)
    response = get_cartodb(query)
    return response.json()['rows'][0]['max']


def main():
    table_name = 'test'
    line_table_name = 'lines'
    data = get_spot_json()
    cartodb_write(data, table_name)
    cartodb_line(data, line_table_name, table_name)

if __name__ == '__main__':
    main()
