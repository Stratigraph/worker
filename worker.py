import requests
import json
import pymysql.converters
import pymysql.cursors
from src import HeatMap
import datetime
import itsdangerous
import time
import configparser
from pprint import pprint

# parsing config
config = configparser.RawConfigParser()
config.read("conf.ini")
conf = {}

for section in config.sections():
    conf[section] = {}
    for pair in config.items(section):
        key, value = pair
        conf[section][key] = value

conv = pymysql.converters.conversions.copy()
conv[246] = float  # convert decimals to floats
conv[10] = str

API_ENDPOINT = conf['shared']['base_url'] + 'api/'


def heatmap_granularity(min_mentions, min_sources, min_sum, time_step, date_from, date_to, search, generate_only=False):
    conf['mysql'].update({'cursorclass': pymysql.cursors.DictCursor,
                          'conv': conv})
    connection = pymysql.connect(**conf['mysql'])  # get min and max date in database

    with connection.cursor() as cursor:
        print("SELECT min(SQLDATE) AS 'min', max(SQLDATE) AS 'max' FROM gdelt_small;")
        cursor.execute("SELECT min(SQLDATE) AS 'min', max(SQLDATE) AS 'max' FROM gdelt_small;")
        gdelt_time = cursor.fetchone()

    gdelt_time['from'] = datetime.datetime.strptime(date_from, "%Y-%m-%d").date()
    gdelt_time['to'] = datetime.datetime.strptime(date_to, "%Y-%m-%d").date()
    gdelt_time['step'] = datetime.timedelta(30)

    result = []
    while gdelt_time['from'] < gdelt_time['to']:
        with connection.cursor() as cursor:
            sql = "SELECT GLOBALEVENTID AS 'key', SQLDATE, ActionGeo_Lat AS 'lat' , ActionGeo_Long AS 'lng' FROM gdelt_small WHERE (SQLDATE BETWEEN {} AND {}) AND NumMentions >= {} AND NumSources >= {} AND SOURCEURL LIKE ('%{}%');".format(
                gdelt_time['from'].strftime("%Y%m%d"), (gdelt_time['from'] + gdelt_time['step']).strftime("%Y%m%d"),
                min_mentions, min_sources, search)
            print(sql)
            cursor.execute(sql)
            heatmap = HeatMap.RedisHeatMap('localhost')
            heatmap_generated = {'data': list(heatmap.gen(cursor.fetchall(), distance=2 * 10 ** 4, min_sum=min_sum)),
                                 'date': gdelt_time['from'].isoformat()}

        result.append(heatmap_generated)
        gdelt_time['from'] += datetime.timedelta(days=time_step)

    return result


if __name__ == "__main__":
    s = itsdangerous.Signer(conf['shared']['sign_key'])

    while True:
        res = requests.get(API_ENDPOINT + 'work')

        if res.status_code == 200:
            payload = res.json()

            slug = payload['slug']
            del payload['slug']
            print(payload)

            r = requests.post(API_ENDPOINT + 'result', data=s.sign(json.dumps({'slug': slug, 'data': heatmap_granularity(**payload)}).encode()))
            print(API_ENDPOINT + 'result', r.status_code)

            print("NOUP")
            time.sleep(5)
        else:
            time.sleep(5)
