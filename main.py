#ques1 - Create a  table attribute dataset and dress dataset
import logging
import  logging as lg

import pandas as pd

lg.basicConfig(filename='log.log', level=logging.DEBUG, format='%(levelname)s,%(asctime)s,%(name)s,%(message)s')


import mysql.connector as conn
try:

    mydb = conn.connect(host = "localhost" , user ="root" , passwd = "Sunny@1997",database='ineurontask',  use_pure=True)
    logging.info("connected to sql")
    cursor = mydb.cursor()
    cursor.execute("create database if not exists ineurontask")
    cursor.execute("create table if not exists ineurontask.attributedatas( Dress_ID int,Style int,Price int,Rating int, Size int, Season varchar(30),NeckLine varchar(30) ,SleeveLength varchar(30),waiseline varchar(30),	Material varchar(30),FabricType varchar(30)	,Decoration varchar(30)	,Pattern_Type varchar(30)	,Recommendation int(20))")
    cursor.execute('create table if not exists ineurontask.dresssales(Dress_ID int(15),`29-8-2013` int(10),`31-8-2013` int(10),`09-02-2013` int(10),`09-04-2013` int(10),`09-06-2013` int(10),`09-08-2013` int(10),`09-10-2013` int(10),`09-12-2013` int(10),`09-14-2013` int(10),`09-16-2013` int(10),`09-18-2013` int(10),`09-20-2013` int(10),`09-22-2013` int(10),`09-24-2013` int(10),`09-26-2013` int(10),`09-28-2013` int(10),`09-30-2013` int(10),`10-02-2013` int(10),`10-04-2013` int(10),`10-06-2013` int(10),`10-08-2013` int(10),`10-10-2013` int(10),`10-12-2013` int(10),primary key(Dress_ID))')
    logging.info("tables created")
except Exception as e:
    logging.error(e)

# ques 2- Do a bulk load for these two table for respective dataset
# ans - bulk load is done through mysqlworkbench through import wizard

# ques 5Store this dataset into mongodb


import pymongo
import pandas
import sqlalchemy

try:
    client = pymongo.MongoClient("mongodb+srv://Sunny:Sunny1997@cluster0.xqvmr.mongodb.net/?retryWrites=true&w=majority")
    db = client.test
    database = client["ineurontask"]
    coll1 = database["attributedata"]
    coll2 = database["dresssales"]
    logging.info("mongodb server is connected")

    att_data = pd.read_sql("select * from attributedatas",mydb)
    logging.info("attribute table to attribute dataframe")
    drs_dta = pd.read_sql("select * from dresssales",mydb)
    logging.info("dresssales table to dresssales dataframe")

    attributedata= att_data.to_dict(orient="records")
    dresssales = drs_dta.to_dict(orient="records")
    logging.info("both datasset converted into dict")
    coll1.insert_many(dresssales)
    coll2.insert_many(attributedata)
except Exception as e:
    logging.error(e)








