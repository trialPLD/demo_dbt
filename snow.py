#!/bin/python3
import snowflake.connector
import pandas as pd
import os
import sys
from snowflake.connector.pandas_tools import write_pandas

# INSERTION du fichier VGI_GAMES dans la table RAW_DATA.VGI_GAMES

# Connection to Snowflake

con = snowflake.connector.connect(
    user="pledain1",
    password="TestSN1!",
    account="xpfxeeo-za86377"
)


def check_connexion(con):
    cs = con.cursor()
    try:
        cs.execute("SELECT current_version()")
        row = cs.fetchone()
        print(row)
    except:
        print("Connection error")
        cs.close()


check_connexion(con)

# vérification du bon fonctionnement - Schema and Database Choice

def make_quick_query(con, query):
    cs = con.cursor()
    try:
        cs.execute(query)
        print("The query {} is successfull".format(query))
    except:
        print("The query {} is successfull".format(query))
    finally:
        cs.close()


make_quick_query(con, "use database STEAM_DB")
make_quick_query(con, "use schema public")


# pas de création de la table, on positionne un auto_create dans le write_table pour récupérer exactement la structure source
#print(create_col(df))
#create_table(con, df, "VGI_GAMES")

def load_data(con, data, name="mytable" ):
    try:
        success, nchunks, nrows, _ = write_pandas( con, data, table_name=name,database="STEAM_DB", schema="RAW_DATA", auto_create_table=True,quote_identifiers= False)
        print("The table {} has been updated".format(name))
        print(nrows , "ont été insérés")
    except:
        print("The table has not been updated")


#récupération du fichier csv
df = pd.read_csv("/home/ubuntu/myenv/vgi_games.csv", sep=",",header=0, index_col=0)
# suppression du "Z" en fin de date
df['released'] = df['released'].str[:-1]

df['rating']=df['rating'].astype(float)

print(df.head())
print(df.info())

df.columns = map(lambda x: str(x).upper(), df.columns)

print(df.columns)

#write_pandas( con, df, table_name="VGI_GAMES",database="STEAM_DB", schema="RAW_DATA",auto_create_table=True,quote_identifiers= False)
# drop de la table existante (write_pandas fait un append)
make_quick_query(con, "drop table raw_data.VGI_GAMES")
# insertion des données du dataframe dans la table
load_data(con, df, "VGI_GAMES")


