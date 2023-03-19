import sqlite3 as sql
import os
import pandas as pd
import functions as f
from datetime import datetime
%pip install mlxtend

os.getcwd()
os.chdir('c:\\Users\\PC\\Desktop\\Analitica 3\\Marketing')

conn=sql.connect('db_movies2')
cur=conn.cursor()


cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())

users=pd.read_sql("""SELECT DISTINCT(userId),COUNT(movieId) as peliculas_vistas  FROM ratings 
            GROUP BY userId; """,conn)

ratings=pd.read_sql("SELECT * FROM ratings; ",conn)

movies=pd.read_sql("SELECT * FROM movies; ",conn)

print('______Users_____')
users.info()
print(users.duplicated().sum())
print('______Ratings_____')
ratings.info()
print(ratings.duplicated().sum())
print('______Movies_____')
movies.info()
print(movies.duplicated().sum())
#--------------------Pre-processing------------------------#
#No hay nulos en ratings :s

#Extract datetime from timestamp and remove this last
ratings['date']=ratings['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
ratings.drop(columns='timestamp',inplace=True)
#______________________#
#---------------Final DFs Exportation-------------------#
from mlxtend.preprocessing import TransactionEncoder

movies=pd.read_sql("""select * from movies""", conn)

genres=movies['genres'].str.split('|')

te = TransactionEncoder()

genres = te.fit_transform(genres)

genres = pd.DataFrame(genres, columns = te.columns_)
a=list(movies.movieId)
genres['movieId']=a
genres


movies_full=pd.merge(movies,genres,on="movieId",how="inner")
movies_full.to_csv('movies_full',index=False)
users.to_csv('users',index=False)
ratings.to_csv('ratings',index=False)



