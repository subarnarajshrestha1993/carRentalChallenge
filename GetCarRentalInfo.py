

import json
import datetime
from pprint import pprint




def read_file_data():

    file_path = input("Enter the file path:")
    fp = open(file_path, 'r')
    file_data = json.load(fp)
    return file_data


def aggregate_data(file_data):
    aggregated_data = list()
    for record in file_data:
        id = record['id']
        event_Type = record['type']
        timestamp = record.get('timestamp')
        comments = record.get('comments')
        ids = [item['id'] for item in aggregated_data]
        if id  in ids:
            # append the other keys
            for new_record in aggregated_data:
                if id == new_record['id']:
                    new_record[event_Type.lower() + "_" + "timestamp"] = timestamp
                    new_record[event_Type.lower() + "_" + "comments"] = comments
        else:
            # insert the keys
            new_record = dict()
            new_record['id'] = id
            new_record[event_Type.lower()+"_"+"timestamp"] = timestamp
            new_record[event_Type.lower() + "_" + "comments"] = comments
            aggregated_data.append(new_record)
    print(aggregated_data)
    return aggregated_data



def print_stats(aggregated_data):
    for record in aggregated_data:
        if record.get('end_timestamp') and 'start_timestamp' in record.keys():
            session_duration = (int(record['start_timestamp'] )- int(record.get('end_timestamp')))/(60*60)
            if session_duration > 24:
                record['over_time'] = True
            else:
                record['over_time'] = False
        else:
            record['over_time'] = False
        if record.get('end_comments'):
            record['car_damaged'] = True
        else:
            record['car_damaged'] = False


    json_string = json.dumps(aggregated_data, indent=4)
    pprint(json_string)
    with open('output_file.json', 'w') as fp:
        fp.write(json_string)

def main():
    file_data = read_file_data()
    aggregated_data = aggregate_data(file_data)
    print_stats(aggregated_data)





if __name__ == "__main__":
    main()