from dotenv import load_dotenv
import os

base_dir = os.path.dirname(__file__)
# print(base_dir)
env_file_path = os.path.join(base_dir, 'config', 'dotenv.env')
# print(env_file_path)
load_dotenv(dotenv_path=env_file_path)

db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')

