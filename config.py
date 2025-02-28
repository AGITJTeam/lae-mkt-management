from datetime import timedelta
import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "default")
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES", 9)))

    @staticmethod
    def get_username():
        return os.getenv("S2_USER")

    @staticmethod
    def get_password():
        return os.getenv("S2_PASS")
