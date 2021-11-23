#Author: Esaú Reyes
#Email: esau.gk.91@gmail.com
#Date: 22/11/2021
#Description: 

'''
    This program will do the following:

    1. Read by batches a JSON file with multiple JSON objects with the following structure:
        {"event_type": 7, "event_time": "2019-01-04 05:07:26", "data": {"user_email": "TAFgZ@gmail.com", "phone_number": "0603728764"}, "processing_date": "2019-01-04"}
    2. Apply the following validations to each JSON object:
        event_type -> int
        event_time -> datetime
        user_email -> string
        phone_number ->  string
        processing_date -> date
       If a object doesn't pass the validations the row will be discarted and written into an error file to be validated later.
    3. Write all the valid rows to a database with the following schema:
        | event_type | event_time | user_email | phone_number | processing_date | file_name |
    
    The variables in the main function that you need to configure:

        file_path = name of the input JSON file
        error_file_path = name of the error file to be created
        db_connection_params = {
            'my_host': , 
            'my_port': , 
            'my_db': , 
            'my_charset': 'utf8mb4',
            'my_user': , 
            'my_password': 
        }
        batch_size = define the size of the batch to process data

'''

import io
import time
import ijson
import pymysql
import logging
from datetime import datetime, date

total_counter_processed = 0
total_counter_written = 0
total_error_found = 0

def datetime_valid(dt_str):
    try:
        datetime.fromisoformat(dt_str)
    except:
        return False
    return True

def date_valid(dt_str):
    try:
        date.fromisoformat(dt_str)
    except:
        return False
    return True

def process_data(data, file_name, field_names, error_file_path):    
    global total_counter_processed, total_error_found
    processed_data = []
    error_data = []
    error_counter = 0
    success_counter = 0

    for row in data:
        values = []
        if  isinstance(row[field_names[0]], int) and \
            datetime_valid(row[field_names[1]]) and \
            isinstance(row[field_names[2]], str) and \
            isinstance(row[field_names[3]], str) and \
            date_valid(row[field_names[4]]):
             #isinstance(datetime.fromisoformat(row[field_names[1]]), datetime) and \        
            #isinstance(date.fromisoformat(row[field_names[4]]), date):

            success_counter += 1
            total_counter_processed += 1

            values.append(row[field_names[0]])
            values.append(row[field_names[1]])
            values.append(row[field_names[2]])
            values.append(row[field_names[3]])
            values.append(row[field_names[4]])
            values.append(file_name)        

            processed_data.append(values)
        else:
            total_error_found += 1
            error_counter += 1
            error_data.append(row)
    
    if len(error_data) > 0:
        my_date = datetime.now().date()
        error_file = error_file_path + str(my_date) + '.json'

        with open(error_file, 'w') as f:
            for item in error_data:
                f.write("%s\n" % item)

        print('Data Types Errors: {}'.format(error_counter))
        logging.info('Data Types Errors: {}'.format(error_counter))
    
    print('Processed records: {}'.format(success_counter))
    logging.info('Processed records: {}'.format(success_counter))

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
        logging.info('The following exception was thrown while trying to connect to the DB: {}'.format(e))
        print(db_connection_params)
        exit(1)

    return connection, cursor

def write_to_db(db_connection_params, processed_data, success_counter):
    global total_counter_written
    connection, cursor = connect_to_db(db_connection_params)
    query = "INSERT INTO user_events VALUES(%s, %s, %s, %s, %s, %s)"
    
    try:
        response = cursor.executemany(query, processed_data)
        connection.commit()
        cursor.close()
        total_counter_written += response
        if success_counter == response:
            print('Written records: {}'.format(response))
            logging.info('Written records: {}'.format(response))
        else:
            print('There were {} records that culd not be written to the DB'.format(processed_data - response))
            logging.info('There were {} records that culd not be written to the DB'.format(processed_data - response))
    except Exception as e:
        print('The following exception was thrown while trying to write to DB: {}'.format(e))        
        logging.info('The following exception was thrown while trying to write to DB: {}'.format(e))
        exit(1)

def process_and_write(line_number, start_time, data, file_name, field_names, error_file_path, db_connection_params):
    print('Line: {}'.format(line_number + 1))
    logging.info('Line: {}'.format(line_number + 1))
    processed_data, success_counter = process_data(data, file_name, field_names, error_file_path)
    process_time = time.time()
    logging.info('Time: {0:.2f}'.format(process_time - start_time))
    print('Time: {0:.2f} seconds'.format(process_time - start_time))
    write_to_db(db_connection_params, processed_data, success_counter)
    logging.info('Time: {0:.2f}'.format(time.time() - process_time))
    print('Time: {0:.2f} seconds'.format(time.time() - process_time))
    print('\n#######################################################')
    return 0

def partial_json_processing(file_path, file_name, batch_size, field_names, error_file_path, db_connection_params):
    data = []
    start_time = time.time()

    with open(file_path) as json_file:
        row_counter = 0
        for line_number, line in enumerate(json_file):
            line_as_file = io.StringIO(line)
            json_parser = ijson.parse(line_as_file)

            for prefix, type, value in json_parser:
                if prefix == 'event_type':
                    event_type = value
                elif prefix == 'event_time':
                    event_time = value
                elif prefix == 'data.user_email':
                    user_mail = value
                elif prefix == 'data.phone_number':
                    phone_number = value
                elif prefix == 'processing_date':
                    processing_date = value

            json_values = {
                'event_type': event_type, 
                'event_time': event_time, 
                'user_email': user_mail, 
                'phone_number': phone_number, 
                'processing_date': processing_date, 
                'file_name': file_name 
            }
            row_counter += 1

            data.append(json_values)

            if row_counter == batch_size:                
                process_and_write(line_number, start_time, data, file_name, field_names, error_file_path, db_connection_params)
                data = []
                row_counter = 0
                start_time = time.time()
        
    if len(data) > 0:        
        process_and_write(line_number, start_time, data, file_name, field_names, error_file_path, db_connection_params)
        data = []
        row_counter = 0
        start_time = time.time()
    
    return 0

def main():
    ############################################ Variables ############################################
    file_path = './docs/202106_flink_data_engieering_sample_data.json'
    error_file_path = './scripts/error_'
    db_connection_params = {
        'my_host': 'localhost', 
        'my_port': 55545, 
        'my_db': 'flink_2021', 
        'my_charset': 'utf8mb4',
        'my_user': 'root', 
        'my_password': 'root'
    }
    batch_size = 2500
    #####################################################################################################
    field_names = ['event_type', 'event_time', 'user_email', 'phone_number', 'processing_date', 'file_name']
    file_name = file_path.split('/')[-1]
    start_time = time.time()

    logging.basicConfig(filename='my_loggging_info.log', encoding='utf-8', format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)
    logging.info('Data Processing Script Starts')

    partial_json_processing(file_path, file_name, batch_size, field_names, error_file_path, db_connection_params)

    print('\nTotal Time: {0:.2f} seconds'.format(time.time() - start_time))
    print('Total Processed Rows: {}'.format(total_counter_processed))
    print('Total Written Records: {}'.format(total_counter_written))
    print('Total Errors Found: {}\n'.format(total_error_found))
    logging.info('Total Execution Time: {0:.2f}'.format(time.time() - start_time))
    logging.info('Total Processed Rows: {}'.format(total_counter_processed))
    logging.info('Total Written Records: {}'.format(total_counter_written))
    logging.info('Total Errors Found: {}'.format(total_error_found))
    logging.info('Data Processing Script Ends')

if __name__ == '__main__':
    main()