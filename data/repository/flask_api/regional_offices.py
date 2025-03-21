from data.repository.calls.compliance_repo import Compliance
import json, redis

def updateRedisKey() -> None:
    """ Updates Redis keys with retrieved regional offices data. """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)
    
    # Generates Regional Offices report.
    offices = Compliance()
    regionalOffices = offices.getRegionalsByOffices()

    # Defines expiration time and formatted data for Redis key.
    expirationTime = 60*60*10
    redisKey = "RegionalsOfficesReport"
    data = json.dumps(obj=regionalOffices, default=str)

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=data, ex=expirationTime)

    # Close Redis connection.
    redisCli.close()
