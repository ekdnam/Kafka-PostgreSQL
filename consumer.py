# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 14:41:59 2020

@author: vgadi
"""

from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        'numtest',
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset = 'earliest',
        enable_auto_commit = True,
        group_id = 'my-group',
        value_deserializer = lambda x: loads(x.decode('utf-8'))
        )

import psycopg2

connection = psycopg2.connect(
        dbname = 'employee',
        user = 'postgres',
        password = 'mandke',
        host = 'localhost',
        port = '5432'
        ) 

# setting the autocommit parameter to true, every statement sent to backend has
# immediate effect

connection.autocommit = True

cursor = connection.cursor()


curr_row_no = 0
total_rows = 1
for message in consumer:
    if curr_row_no < total_rows:
        message = message.value
        message = message.split(",")
        #print(value)
        insert_command = "insert into new_db(em_name, em_id) values(" + message[0] + " , " + message[1] + ");"
        cursor.execute(insert_command)
        curr_row_no += 1
    else:
        break