from pymongo import MongoClient
import threading
from threading import Thread
import pymysql.cursors
import requests
import json

connection = pymysql.connect(host='127.0.0.1',user='root',password='',db='dumpml',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
#cliente = MongoClient('localhost', 27017)

def updateMongoDb(collection,data,idMl):       
    collection.find_one_and_update(   
        {'id': idMl},     
        {'$set':data},      
        upsert=True,   
    )

def insertIDs(id):
    sql = "INSERT IGNORE INTO `mlcode` (`code`) VALUES ('"+ str(id) +"')" 
    print(sql)
    cursor.execute(sql)
    connection.commit()
    return

def selectMlCode():
    sql = "SELECT url FROM url WHERE status IS NULL LIMIT 1"         
    cursor.execute(sql)
    return cursor.fetchall()

def updateDb(idMlCode):
    sql = "UPDATE url SET status=1 WHERE url='"+ str(idMlCode) +"'" 
    cursor.execute(sql)
    connection.commit()
    return

def requestApi(mlCode):
    print(mlCode['url'])
    html = requests.get('https://api.mercadolibre.com/sites/MLB/search?category=MLB22736&condition=new&offset=0')    
    if (html.status_code == 200):
        load = json.loads(html.content)       
        for resultado in load['results']:           
            insertIDs(resultado['id'])                     
            #updateMongoDb(collection,resultado,resultado['id'])
        updateDb(mlCode['url']) 
    elif(html.status_code ==  400):
        updateDb(mlCode['url'])


try: 
    with connection.cursor() as cursor:
        #banco = cliente.mercadolivre
        #collection = banco.info
        #collection = banco.teste

        dataML = selectMlCode()

        while dataML != None:                      
            for mlCode in dataML:                
                requestApi(mlCode)
                #requestApi(mlCode,collection)                                                                           
            dataML = selectMlCode()    
finally:       
    connection.close()