from pymongo import MongoClient
import threading
from threading import Thread
import pymysql.cursors
import requests
import json

connection = pymysql.connect(host='127.0.0.1',user='root',password='',db='dumpml',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
cliente = MongoClient('localhost', 27017)

def updateMongoDb(collection,data,idMl):       
    collection.find_one_and_update(   
        {'id': idMl},     
        {'$set':data},      
        upsert=True,   
    )

def selectMlCode():
    sql = "SELECT id,code FROM `mlcode` WHERE `statusInfo` IS NULL ORDER BY `id` ASC LIMIT 1000"
    #sql = "SELECT id,code FROM mlcode WHERE statusInfo IS NULL LIMIT 1"         
    cursor.execute(sql)
    return cursor.fetchall()

def updateDb(idMlCode):
    sql = "UPDATE mlcode SET statusInfo=1 WHERE id='"+ str(idMlCode) +"'" 
    cursor.execute(sql)
    connection.commit()
    return

def requestApi(mlCode,collection):
    print(mlCode['code'])
    html = requests.get('https://api.mercadolibre.com/items/'+mlCode['code'])
    if (html.status_code == 200):
        load = json.loads(html.content)    
        updateMongoDb(collection,load,mlCode['code'])
    elif(html.status_code ==  400):
        updateDb(mlCode['id'])


try: 
    with connection.cursor() as cursor:
        banco = cliente.mercadolivre
        collection = banco.info

        dataML = selectMlCode()

        while dataML != None:                      
            for mlCode in dataML:
                requestApi(mlCode,collection)
                updateDb(mlCode['id'])
                #numberThread = threading.active_count()
                #if(numberThread < 200):
                #   Thread(target=requestApi,args=[mlCode,collection]).start()                 
                #   updateDb(mlCode['id'])                                
            dataML = selectMlCode()
finally:       
    connection.close()