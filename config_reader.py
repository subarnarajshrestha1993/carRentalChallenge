import yaml
from pprint import pprint

def read_config():
    data = None
    try:
        with open(r"config/db_config.yaml") as file:
            data = yaml.safe_load(file)

        return data
    except Exception as e:
        print(f"Exception occured while reading config file: {e}")


# if __name__ == '__main__':
#     pprint(read_config())