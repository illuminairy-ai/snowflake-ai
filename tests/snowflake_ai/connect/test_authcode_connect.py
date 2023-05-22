
from snowflake_ai.connect import AuthCodeConnect
import re

def test_init_default():
    pass


if __name__ == '__main__':
    acc = AuthCodeConnect()
    print(f"DEBUG => {acc.authorize_request()}")