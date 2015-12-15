import requests
import json
import pymysql.converters
import pymysql.cursors
from src import HeatMap
import datetime
import itsdangerous
import time

conv = pymysql.converters.conversions.copy()
conv[246] = float  # convert decimals to floats
conv[10] = str

host = 'localhost'

API_ENDPOINT = 'http://147.251.253.253/api/'


def heatmap_granularity(min_mentions, min_sources, min_sum, time_step, date_from, date_to, search, generate_only=False):
    connection = pymysql.connect(host=host,
                                 user='hkarasek',
                                 password='6Mm8YiHVkk71kViujdseFkyAWlADLfZpdei0jyc0Lf4=',
                                 db='gdelt',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor,
                                 conv=conv)  # get min and max date in database

    with connection.cursor() as cursor:
        cursor.execute("SELECT min(gdelt.date) AS 'min', max(gdelt.date) AS 'max' FROM gdelt;")
        gdelt_time = cursor.fetchone()

    gdelt_time['from'] = datetime.datetime.strptime(date_from, "%Y-%m-%d").date()
    gdelt_time['to'] = datetime.datetime.strptime(date_to, "%Y-%m-%d").date()
    gdelt_time['step'] = datetime.timedelta(30)

    result = []
    while gdelt_time['from'] < gdelt_time['to']:
        with connection.cursor() as cursor:
            sql = "SELECT id AS 'key', gdelt.date, gdelt.ActionGeo_Lat AS 'lat' , gdelt.ActionGeo_Long AS 'lng' FROM gdelt WHERE (gdelt.date BETWEEN {} AND {}) AND gdelt.NumMentions >= {} AND gdelt.NumSources >= {} AND gdelt.SourceUrl LIKE ('%{}%');".format(
                gdelt_time['from'].strftime("%Y%m%d"), (gdelt_time['from'] + gdelt_time['step']).strftime("%Y%m%d"),
                min_mentions, min_sources, search)
            print(sql)
            cursor.execute(sql)
            heatmap = HeatMap.RedisHeatMap(host)
            heatmap_generated = {'data': list(heatmap.gen(cursor.fetchall(), distance=2 * 10 ** 4, min_sum=min_sum)),
                                 'date': gdelt_time['from'].isoformat()}

        result.append(heatmap_generated)
        gdelt_time['from'] += datetime.timedelta(days=time_step)

    return result


if __name__ == "__main__":
    s = itsdangerous.Signer("ee09f5d40551658fe6a3c52f3a9ede9769604fce1986a3af0a8a05694f32")

    while True:
        res = requests.get(API_ENDPOINT + 'work')

        if res.status_code == 200:
            payload = res.json()

            slug = payload['slug']
            del payload['slug']
            print(payload)

            r = requests.post(API_ENDPOINT + 'file',
                              data=s.sign(json.dumps({'slug': slug, 'data': heatmap_granularity(**payload)}).encode()))
            print(r.status_code)

            print("NOUP")
            time.sleep(30)
        else:
            time.sleep(1)
