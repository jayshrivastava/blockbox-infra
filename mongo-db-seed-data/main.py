import csv
import json
import pymongo
from pymongo import MongoClient

# Open files
with open('data/movies.csv', 'r', encoding = 'utf-8-sig') as f:
  reader = csv.reader(f)
  movies = list(reader)
  movies.pop(0)

with open('data/ratings.csv', 'r', encoding = 'utf-8-sig') as f:
  reader = csv.reader(f)
  ratings = list(reader)
  ratings.pop(0)

# Get Environment Variables
with open('env.json') as env_file:  
    env = json.load(env_file)



# Global Vars
movies_dict = {} # movie id as key
users_dict = {}       # user id as key
oldest_movie_date = 1990
client = MongoClient(env['MONGO_DB_URI']) # mongo DB client
database = client['test']  # connect to test database
users_collection = database['users_3']
movies_collection = database['movies_3']

# Init Movies Dict
for i, movie in enumerate(movies):
    title = movie[1]
    id = movie[0]
    genres = movie[2].split('|')

    # Safely get the movie date. Add movie to movies_dict if the movie release date is greater or equal to oldest_movie_date
    date_chunk = title[len(title)-8:len(title)]
    tmp1 = date_chunk.split('(')
    if (len(tmp1) > 1):
        year = int(tmp1[1].split(')')[0])
        if year >= oldest_movie_date:
            movies_dict[id] = {
                "title": title,
                "genres": genres
            }
   
# Populate ratings in the movies dict and users dict IF the movie rated is in the movies dict
for i, rating in enumerate(ratings):
    movie_id = rating[1]
    user_id = rating[0]
    rating_val = rating[2]

    # add rating to users dict
    if movie_id in movies_dict:
        if user_id not in users_dict:
            users_dict[user_id] = {}
        
        users_dict[user_id][movie_id] = float(rating_val)/5

    # add ratings to movies dict
    if movie_id in movies_dict:
        if 'ratings' not in movies_dict[movie_id]:
            movies_dict[movie_id]['ratings'] = {}
        movies_dict[movie_id]['ratings'][user_id] =  float(rating_val)/5

Populate ratings for movies which users have not rated
for movie_id in movies_dict:
    for user_id in users_dict:
        if movie_id not in users_dict[user_id]:
            users_dict[user_id][movie_id] = 0

# Map Movies dict and users dict to arrays
users_to_insert = [ { 'user_id': user_id, 'ratingsIndexedByMovieId': users_dict[user_id], 'name': '' } for user_id in users_dict.keys()]
movies_to_insert = [] 
for movie_id in movies_dict:
    movie_to_insert = { 'movie_id': movie_id, 'title': movies_dict[movie_id]['title'], 'genres': movies_dict[movie_id]['genres'] }
    if 'ratings' in movies_dict[movie_id]:
        movie_to_insert['ratingsIndexedByUserId'] = movies_dict[movie_id]['ratings']
    movies_to_insert.append(movie_to_insert)

    
# Insert movies and users into database
result = users_collection.insert_many(users_to_insert)
result = movies_collection.insert_many(movies_to_insert)