# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 13:44:04 2020

@author: vgadi
"""

from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        value_serializer = lambda x: dumps(x).encode('utf-8')
        )


import psycopg2

connection = psycopg2.connect(
        dbname = 'employee',
        user = 'postgres',
        password = 'mandke,
        host = 'localhost',
        port = '5432'
        ) 

# we can use an object of the cursor class to execute postgres code in a db 
# session

# as the name has not been specified, it is a client side cursor
cursor = connection.cursor()

# the query can be modified as per the requirement
query = "select * from employee;"

cursor.execute(query)

# returns a sequence of columns
desc = cursor.description

# fetch all returns all the rows of a query result
rows = cursor.fetchall()

# iterating over the rows
for row in rows:
    info = ""
    for col in range(len(desc)):
        info += str(row[col]) + "   "
    print(info)
    producer.send(topic = 'numtest', value = info ) 
    sleep(2)