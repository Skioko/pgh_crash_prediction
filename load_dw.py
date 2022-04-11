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

def load_dw():
    # Connect to MySQL database
    odb_conn = None
    adb_conn = None
    adb_load_query = "INSERT INTO fact(locid,prodid,sale) " \
                     "VALUES(%s,%s,%s)"
    odb_aggregate_query = "SELECT location, product, sum(sale) "\
                          " FROM transaction "\
                          " GROUP BY location, product"    
                                     
    try:  
        odb_conn = mysql.connector.connect(host='192.168.1.10', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  database = 'odb',
                                  user='deuser',
                                  password='depassword')
        
        dw_conn = mysql.connector.connect(host='192.168.1.10', # !!! make sure you use your VM IP here !!!
                                  port=23306, 
                                  database = 'dw',
                                  user='deuser',
                                  password='depassword')
        
        if odb_conn.is_connected():
                print('Connected to source ODB MySQL database')
                
        if dw_conn.is_connected():
                print('Connected to destination DW MySQL database')
        
        odb_cursor = odb_conn.cursor()
        dw_cursor = dw_conn.cursor()
        
        odb_cursor.execute(odb_aggregate_query)
        aggr_tuples = odb_cursor.fetchall()
    
        dw_cursor.executemany(adb_load_query,aggr_tuples)
        
        dw_conn.commit()
        
        dw_cursor.execute("SELECT count(*) FROM fact")
        res = dw_cursor.fetchone()
    
        print('DW is loaded: {} new tuples are inserted'.format(len(aggr_tuples)))
        print('               {} total tuples are inserted'.format(res[0]))    
        
            
    except Error as e:
        print(e)
        
    finally:
        if odb_conn is not None and odb_conn.is_connected():
            odb_cursor.close()
            odb_conn.close()
        if dw_conn is not None and dw_conn.is_connected():
            dw_cursor.close()
            dw_conn.close()    
            
if __name__ == '__main__':
    load_dw()
    