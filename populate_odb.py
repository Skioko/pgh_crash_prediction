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

def populate_odb():
    # Connect to MySQL database
    conn = None
    query = "INSERT INTO crash_weather_hazards (hazardId,hazard_type,user_category,latitude,longitude) " \
            "VALUES(%s,%s,%s,%s,%s)"
    tuples = [('jones','loc1','prod1', 10),('smith','loc1','prod1', 20),('jones','loc1','prod1', 10)]        
    try:  
        conn = mysql.connector.connect(host='localhost', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  database = 'odb',
                                  user='deuser',
                                  password='depassword')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        
        for tuple in tuples:
            cursor.execute(query,tuple)
            
        conn.commit()
        
        cursor.execute("SELECT count(*) FROM transaction")
        res = cursor.fetchone()
    
        print('ODB is populated: {} new tuples are inserted'.format(len(tuples)))
        print('                  {} total tuples are inserted'.format(res[0]))    
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()
            
if __name__ == '__main__':
    populate_odb()
    