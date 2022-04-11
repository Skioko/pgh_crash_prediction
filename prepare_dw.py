#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: vladimir
Make sure you have mysql-connector installed:
    pip install mysql-connector-python
Also, make sure you created a mysql user deuser with password depassword and granted your user all privileges    
"""

import mysql.connector
from mysql.connector import Error

def prepare_dw():
    # Connect to MySQL database
    conn = None
    drop_db = "DROP DATABASE IF EXISTS crash_weather_dw"
    create_db = " CREATE DATABASE crash_weather_dw"
    use_db = "use crash_weather_dw"
    create_table = "CREATE TABLE fact (factId INT NOT NULL AUTO_INCREMENT PRIMARY KEY, crashId INT NOT NULL " \
                     "latitude DECIMAL(18,15), longitude DECIMAL(18,15), severity INT)"

    try:  
        conn = mysql.connector.connect(host='localhost', # !!! make sure you use your VM IP here !!!
                                  port=23306, 
                                  user='deuser',
                                  password='depassword')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        cursor.execute(drop_db)
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_table)
        
        conn.commit()

        print('DW is prepared')
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()
            
if __name__ == '__main__':
    prepare_dw()
    