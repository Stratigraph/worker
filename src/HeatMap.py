import redis


class BetaRedis(redis.StrictRedis):
    def georadius(self, name, *values):
        return self.execute_command('GEORADIUS', name, *values)

    def geoadd(self, name, *values):
        return self.execute_command('GEOADD', name, *values)

    def geopos(self, name, *values):
        return self.execute_command('GEOPOS', name, *values)


class RedisHeatMap:
    REDIS_KEY = 'heatmap'
    REDIS_KEY_GEO = REDIS_KEY + '_GEO'
    REDIS_KEY_HASH = REDIS_KEY + '_HASH'

    def __init__(self, host='localhost', port=6379, db=0):
        self.r = BetaRedis(host=host, port=port, db=db)
        self.r.flushdb()

    def gen(self, data, distance=200000, min_sum=1):
        for point in data:
            try:
                res = self.r.georadius(self.REDIS_KEY_GEO, point['lng'], point['lat'], distance, 'm')
                if not res:
                    self.r.geoadd(self.REDIS_KEY_GEO, point['lng'], point['lat'], point['key'])
                    self.r.hset(self.REDIS_KEY_HASH, point['key'], 1)
                else:
                    self.r.hincrby(self.REDIS_KEY_HASH, res[0])
            except redis.exceptions.ResponseError as e:
                pass

        for key in self.r.hscan_iter(self.REDIS_KEY_HASH):
            lng, lat = map(lambda x: x.decode(), self.r.geopos(self.REDIS_KEY_GEO, key[0].decode())[0])
            if int(key[1]) >= min_sum:
                yield {'key': key[0].decode(), 'lat': lat, 'lng': lng, 'sum': int(key[1])}
