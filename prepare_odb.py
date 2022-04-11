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

def prepare_odb():
    # Connect to MySQL database
    conn = None
    drop_db = "DROP DATABASE IF EXISTS crash_weather_odb"
    create_db = " CREATE DATABASE crash_weather_odb"
    use_db = "use crash_weather_odb"
    create_table = "CREATE TABLE crash_weather_hazards (hazardId INT NOT NULL AUTO_INCREMENT PRIMARY KEY, hazard_type VARCHAR(20), " \
                     "user_category VARCHAR(20), latitude DECIMAL(18,15), longitude DECIMAL(18,15))"

    try:  
        conn = mysql.connector.connect(host='localhost', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  user='deuser',
                                  password='depassword')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        cursor.execute(drop_old)
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_table)
        
        conn.commit()

        print('CW_ODB is prepared')
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()
            
if __name__ == '__main__':
    prepare_odb()
    