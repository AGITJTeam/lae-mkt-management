import logging, redis, redis.exceptions

logger = logging.getLogger(__name__)

def initRedis() -> redis.Redis | None:
    """ Creates a Redis client and checks if it's working.

    Returns
        {redis.Redis | None} the Redis client if it's working, None 
        otherwise.
    """
    
    try:
        redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)
        redisCli.ping()
        logger.info("Redis connection established.")
        return redisCli
    except redis.exceptions.ConnectionError:
        logger.error("Redis connection error. Check if Redis container is running.")
        return None

redisCli = initRedis()
