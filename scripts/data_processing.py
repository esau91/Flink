#Author: Esaú Reyes
#Email: esau.gk.91@gmail.com
#Date: 19/11/2021
#Description: 

import io
import json
import time
import ijson
import pymysql
import logging
from datetime import datetime, date

# One record from input file
# {"event_type": 7, "event_time": "2019-01-04 05:07:26", "data": {"user_email": "TAFgZ@gmail.com", "phone_number": "0603728764"}, "processing_date": "2019-01-04"}

def process_data(data, file_name, field_names, error_file_path):    
    processed_data = []
    error_data = []
    error_counter = 0
    success_counter = 0

    for row in data:
        values = []
        if  isinstance(row[field_names[0]], int) and \
            isinstance(datetime.fromisoformat(row[field_names[1]]), datetime) and \
            isinstance(row['data'][field_names[2]], str) and \
            isinstance(row['data'][field_names[3]], str) and \
            isinstance(date.fromisoformat(row[field_names[4]]), date):

            success_counter += 1

            values.append(row[field_names[0]])
            values.append(row[field_names[1]])
            values.append(row['data'][field_names[2]])
            values.append(row['data'][field_names[3]])
            values.append(row[field_names[4]])
            values.append(file_name)        

            processed_data.append(values)
        else:
            error_counter += 1
            error_data.append(row)
    
    if len(error_data) > 1:
        my_date = datetime.now().date()
        error_file = error_file_path + my_date + '.json'

        with open(error_file, 'w') as f:
            for item in error_data:
                f.write("%s\n" % item)

        print('\n{} datatypes error were found, check out the file: {}'.format(error_counter, error_file))
    
    print('\nProcessed records: {}'.format(success_counter))

    return processed_data, success_counter
            
def connect_to_db(db_connection_params):

    try:
        connection = pymysql.connect(
                            host = db_connection_params['my_host'],
                            user = db_connection_params['my_user'],                                            
                            passwd = db_connection_params['my_password'],
                            database = db_connection_params['my_db'],
                            port = db_connection_params['my_port'],
                            charset = db_connection_params['my_charset'],
                            cursorclass = pymysql.cursors.DictCursor
                            )
        cursor = connection.cursor()
    except Exception as e:
        print('The following exception was thrown while trying to connect to the DB: {}'.format(e))
        print(db_connection_params)
        exit(1)

    return connection, cursor

def write_to_db(db_connection_params, processed_data, success_counter):
    connection, cursor = connect_to_db(db_connection_params)
    query = "INSERT INTO user_events VALUES(%s, %s, %s, %s, %s, %s)"
    
    try:
        response = cursor.executemany(query, processed_data)
        connection.commit()
        cursor.close()
        if success_counter == response:
            print('\nWritten records: {}'.format(response))
        else:
            print('\nThere were {} records that culd not be written to the DB'.format(processed_data - response))
    except Exception as e:
        print('The following exception was thrown while trying to write to DB: {}'.format(e))        
        exit(1)

def read_json(file_path):
    data = []
    for line in open(file_path, 'r'):
        data.append(json.loads(line))

    return data

def partial_json_reading(file_path):
    data = []

    with open(file_path) as json_file:
        cursor = 0
        for line_number, line in enumerate(json_file):
            print('Line: {}, Cursor: {}'.format(line_number+1, cursor))
            line_as_file = io.StringIO(line)
            json_parser = ijson.parse(line_as_file)

            for prefix, type, value in json_parser:
                if prefix in ['event_type', 'event_time', 'data.user_email', 'data.phone_number', 'processing_date']:
                    print('Prefix: {}, Type: {}, Value: {}'.format(prefix, type, value))
            cursor += len(line)

        return data

def main():
    file_path = '/Users/esau/Downloads/Flink/202106_flink_data_engieering_sample_data.json'
    error_file_path = '/Users/esau/Downloads/Flink/error_'
    field_names = ['event_type', 'event_time', 'user_email', 'phone_number', 'processing_date', 'file_name']
    file_name = file_path.split('/')[-1]
    db_connection_params = {
        'my_host': 'localhost', 
        'my_port': 55545, 
        'my_db': 'flink_2021', 
        'my_charset': 'utf8mb4',
        'my_user': 'root', 
        'my_password': 'root'
    }
    
    logging.basicConfig(filename='my_loggging_info.log', encoding='utf-8', format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)
    logging.info('Data Processing Script Starts')

    start_time = time.time()
    data = read_json(file_path)
    processed_data, success_counter = process_data(data, file_name, field_names, error_file_path)
    process_time = time.time()

    logging.info('Data Processing Time: {0:.2f}'.format(process_time - start_time))
    print('\nTime: {0:.2f} seconds'.format(process_time - start_time))

    write_to_db(db_connection_params, processed_data, success_counter)

    logging.info('Data Writing Time: {0:.2f}'.format(time.time() - process_time))
    print('\nTime: {0:.2f} seconds'.format(time.time() - process_time))
    print('\nTotal Time: {0:.2f} seconds\n'.format(time.time() - start_time))
    logging.info('Total Execution Time: {0:.2f}'.format(time.time() - start_time))
    logging.info('Data Processing Script Ends')


if __name__ == '__main__':
    main()