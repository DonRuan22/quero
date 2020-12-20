# -*- coding: utf-8 -*-
"""
Created on Sat Dec 19 10:55:17 2020

@author: Donruan
"""

#import requests
import pandas as pd
import certifi
import urllib3
import json
from urllib3 import request
from pandas.io.json import json_normalize
import psycopg2


def extract_data():

    http = urllib3.PoolManager(
           cert_reqs='CERT_REQUIRED',
           ca_certs=certifi.where())
    
    url = 'http://dataeng.quero.com:5000/caged-data'
    try:
        r = http.request('GET', url)
    except urllib3.exceptions.HTTPError as e:
      print('Request failed:', e.reason)
    
    data_dict= json.loads(r.data.decode('utf-8'))
    if not data_dict['success']:
        SystemExit('Error: failed')
        
    df = json_normalize(data_dict, 'caged')
    del data_dict
    
    
    df.drop_duplicates(keep='first',inplace=True) 
    df['salario'] = df['salario'].apply(lambda x: x.replace(",", ""))
    
    df = df.astype({'categoria':'int32',
                    'cbo2002_ocupacao':'int64',
                    'competencia':'int64',
                    'fonte':'int32',
                    'grau_de_instrucao':'int32',
                    'horas_contratuais':'int32',
                    'id':'int64',
                    'idade':'int32',
                    'ind_trab_intermitente':'int32',
                    'ind_trab_parcial':'int32',
                    'indicador_aprendiz':'int32',
                    'municipio':'int64',
                    'raca_cor':'int32',
                    'regiao': 'int32',
                    'salario': 'float64',
                    'saldo_movimentacao':'int32',
                    'secao':'str',
                    'sexo':'int32',
                    'subclasse':'int64',
                    'tam_estab_jan':'int32',
                    'tipo_de_deficiencia':'int32',
                    'tipo_empregador':'int32',
                    'tipo_estabelecimento':'int32',
                    'tipo_movimentacao':'int32',
                    'uf':'int32'})
        
    df['salario'] = df['salario'].round(decimals=3)
    
    return df
    
    


def create_tables(nameTable, dictColumns):
    """ create tables in the PostgreSQL database"""
    stringCommand="""
        CREATE TABLE """+ nameTable+ """ (
        """
    for key, value in dictColumns.items():
        if key =='id':
            stringFeature = key + ' ' + 'INT PRIMARY KEY,\n'
        else:
            stringFeature = key + ' ' + value + ',\n'
        stringCommand = stringCommand + stringFeature
    stringCommand = stringCommand[:-2]
    stringCommand = stringCommand + """)
            """
    command = (stringCommand)
        
    conn = None
    try:
        # read the connection parameters
        #Establishing the connection
        conn = psycopg2.connect(
           database="testdb", user='postgres', password='rdonino', host='127.0.0.1', port= '5432'
        )
        cur = conn.cursor()
        # create table
        cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def bulkInsert(records, nameTable, dictColumns):
    tuple_data = [tuple(x) for x in records.to_numpy()]
    try:
        conn = psycopg2.connect(
           database="testdb", user='postgres', password='rdonino', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()
        
        stringCommandIns="""
        INSERT INTO """+ nameTable+ """ (
        """
        
        stringCommandValue="""
        VALUES (
        """
        stringValue = '%s,'
        for key, value in dictColumns.items():
            stringFeature = key+ ',\n'
            stringCommandIns = stringCommandIns + stringFeature
            stringCommandValue = stringCommandValue + stringValue
                
        stringCommandIns = stringCommandIns[:-2]
        stringCommandIns = stringCommandIns + """)
                """
        stringCommandValue = stringCommandValue[:-1]
        stringCommandValue = stringCommandValue + """)
            """
        sql_insert_query = stringCommandIns +'\n'+ stringCommandValue

        # executemany() to insert multiple rows rows
        result = cursor.executemany(sql_insert_query, tuple_data)
        conn.commit()
        print(cursor.rowcount, "Record inserted successfully into table")

    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))

    finally:
        # closing database connection.
        if (conn):
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
            

def createIdx(nameTable, idxFeat):
    try:
        conn = psycopg2.connect(
           database="testdb", user='postgres', password='rdonino', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()
        
        stringCommand="""
        CREATE INDEX idx_"""+idxFeat +""" ON """+ nameTable+ """("""+idxFeat+""")
        """
        
        cursor = conn.cursor()
        # create idx
        cursor.execute(stringCommand)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    dictFeat = {'categoria':'INT',
                    'cbo2002_ocupacao':'INT',
                    'competencia':'INT',
                    'fonte':'INT',
                    'grau_de_instrucao':'INT',
                    'horas_contratuais':'INT',
                    'id':'INT',
                    'idade':'INT',
                    'ind_trab_intermitente':'INT',
                    'ind_trab_parcial':'INT',
                    'indicador_aprendiz':'INT',
                    'municipio':'INT',
                    'raca_cor':'INT',
                    'regiao': 'INT',
                    'salario': 'float(3)',
                    'saldo_movimentacao':'INT',
                    'secao':'VARCHAR(255)',
                    'sexo':'INT',
                    'subclasse':'INT',
                    'tam_estab_jan':'INT',
                    'tipo_de_deficiencia':'INT',
                    'tipo_empregador':'INT',
                    'tipo_estabelecimento':'INT',
                    'tipo_movimentacao':'INT',
                    'uf':'INT'}
    #data = extract_data()
    #create_tables('test', dictFeat)
    #bulkInsert(data, 'test', dictFeat)
    #createIdx('test', 'regiao')
    createIdx('test', 'idade')
    test =0