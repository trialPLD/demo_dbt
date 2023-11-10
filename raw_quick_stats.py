#!/bin/python3
import snowflake.connector
import pandas as pd
import os
import sys
from snowflake.connector.pandas_tools import write_pandas


# Connection to Snowflake

con = snowflake.connector.connect(
    user="pledain",
    password="TestSN1!",
    account="whntvst-so25644"
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
make_quick_query(con, "use schema RAW")


def correct_json(file1,file2) :
   # lit le fichier et ajoute un crochet en début et fin
   with open(file1, 'r+') as f1, open(file2, 'w') as f2:
        first_char = f1.read(1)
        
        if first_char != '[':
            content = f1.read()
            f1.seek(0, 0)
            f2.write('[{' + content + ']')
        

def get_quick_stats(filename):
  """
  entrée :  identifiant du jeu
  sortie :  données de l_quick_stats_url.json
  """
  file="/home/ubuntu/myenv/Games5/{}"
  correct_file="/home/ubuntu/myenv/Games5/correct/{}"
  #le fichier quickstart est mal structuré , il manque les crochets en début et fin de fichier ce qui empeche de l'interpréter ensuite correctement
  correct_json(file.format(filename), correct_file.format(filename))
  
  try:
    data=pd.read_json(correct_file.format(filename), orient='records')
    data['stream_id']=filename.split('_')[0]
    
  except:
    print("fichier écarté",filename)
    data=pd.DataFrame()
    data['stream_id']=filename.split('_')[0]
  
  return data

game_quick_stats=[]

files = os.listdir("/home/ubuntu/myenv/Games5/")

for filename in files:
    if "quick" in filename :
        game_quick_stats.append(get_quick_stats(filename))
        print(filename)


df_games_quick_stats= pd.concat(game_quick_stats)
df_games_quick_stats.reset_index()
print(df_games_quick_stats.head()) 



def load_data(con, data, name="mytable" ):
    
    try:
        success, nchunks, nrows, _ = write_pandas( con, data, table_name=name,database="STEAM_DB", schema="RAW",auto_create_table=True)
        print("The table {} has been updated".format(name))
        print(nrows , "ont été insérés")
    except:
        print("The table has not been updated")

load_data(con, df_games_quick_stats, "QUICK_STATS")