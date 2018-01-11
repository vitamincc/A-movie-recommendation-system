#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 20:10:52 2017

@author: xucc
"""

from pyspark.mllib.recommendation import ALS, Rating, MatrixFactorizationModel
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from pyspark import SparkContext
import math


#%%
sc = SparkContext()
rating = sc.textFile('ratings.csv')
rating_header = rating.take(1)[0]
complete_rating = rating.filter(lambda line: line!=rating_header)\
     .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()


#print samples (userId, movieId, rating)
complete_rating.take(3)

#%%part 1: calculate RMSE, MSE, MAP
#read movie data
movies = sc.textFile('movies.csv')
movies_header = movies.take(1)[0]
movies_data = movies.filter(lambda line: line!=movies_header)\
    .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1])).cache()

#print samples (movieId, movieTitle)
movies_title = movies_data.map(lambda x: (int(x[0]),x[1]))
movies_title.take(3)

#%%generate 5 folds for training and validation
    
validate1, validate2, validate3, validate4, validate5 = complete_rating.randomSplit([0.2,0.2,0.2,0.2,0.2], seed = 0L)

train1 = complete_rating.subtract(validate1)
train2 = complete_rating.subtract(validate2)
train3 = complete_rating.subtract(validate3)
train4 = complete_rating.subtract(validate4)
train5 = complete_rating.subtract(validate5)


    
def counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings

#(movieId, rating)
movie_ID_rating = (complete_rating.map(lambda x: (x[1], x[2])).groupByKey())
movie_ID_avg_rating = movie_ID_rating.map(counts_and_averages)

#print (movieId, rated movie number, movie rating average))
movie_ID_avg_rating.take(5)

#(movieId, rated movies number)
movie_rating_counts = movie_ID_avg_rating.map(lambda x: (x[0], x[1]))


def order_movies(ID_movie_ratings_tuple):
    movie_seq = list()
    #movie_seq = ID_movie_ratings_tuple[1].sortBy(lambda x: x[1],False)
    movie_order = sorted(ID_movie_ratings_tuple[1], key = lambda x: x[1], reverse=True)
    for seq, movie_rating in enumerate(movie_order):
        temp = (movie_rating[0], seq)
        movie_seq.append(temp)
    return ID_movie_ratings_tuple[0],movie_seq


def movie_index(movie_ratings_tuple):
    dict0 = dict(movie_ratings_tuple[0])
    dict1 = dict(movie_ratings_tuple[1])
    temp = [(k, dict0[k], dict1[k]) for k in sorted(dict0)]
    l1 = list()
    l2 = list()
    for i in temp:
        l1.append(i[1])
        l2.append(i[2])
        if len(l1) > 10:
            break
    return (l1,l2)

numIterations = 10
regulz_para = 0.1
ranks = [5,10,15]
errors = list()
tolerance = 0.02
best_rank = -1
best_iteration = -1


def bestmodel(traindata, validatedata):
    bestValidationRmse = float("inf")
    #map validate data to userId, movieId
    validation = validatedata.map(lambda r: (r[0], r[1]))
    #get actual rating data for pairs of userId, movieId
    ratingTuple = validatedata.map(lambda r: ((int(r[0]), int(r[1])), float(r[2])))
    for rank in ranks:
        #create model by train data
        model = ALS.train(traindata, rank, numIterations,lambda_ = regulz_para) 
        #predict ratings for validation data
        predictions = model.predictAll(validation).map(lambda r: ((r[0], r[1]), r[2]))      
        #create predict and actual ratings
        scoreAndLabels = predictions.join(ratingTuple).map(lambda tup: tup[1])
        
        regMetrics = RegressionMetrics(scoreAndLabels)
        RMSE = regMetrics.rootMeanSquaredError
        MSE = regMetrics.meanSquaredError
        
        print("For rank %s:" % rank)
        print("RMSE = %s" % RMSE)
        print("MSE = %s" % MSE)
        
        
        if RMSE < bestValidationRmse:
            bestValidationRmse = RMSE
            best_rank = rank
            
        
    print 'The best model was trained with rank %s' % best_rank
    
    #MAP: 
    #actual top 10 movie sequence for users by rating
    model = ALS.train(traindata, best_rank, numIterations,lambda_ = regulz_para)
    actual_user_movie = validatedata.map(lambda x: (x[0], (x[1], x[2]))).groupByKey()
    actual_user_movie1 = actual_user_movie.map(order_movies) 
    predict_user_movie = model.predictAll(validation).map(lambda r: (r[0], (r[1], r[2]))).groupByKey()
    predict_user_movie1 = predict_user_movie.map(order_movies) 
    movie_seq = predict_user_movie1.join(actual_user_movie1).map(lambda x:x[1])
    movie_seq = movie_seq.map(movie_index)
    rankMetrics = RankingMetrics(movie_seq)
    MAP = rankMetrics.meanAveragePrecision
    print("MAP = %s" % MAP)
        
        
bestmodel(train1, validate1)
bestmodel(train2, validate2) 
bestmodel(train3, validate3) 
bestmodel(train4, validate4)        
bestmodel(train5, validate5)         
        
        

#%% part 2: recommendation movies for the new user
#insert new user
new_Id = 0
new_user_ratings = [
     (0,260,4), # Star Wars (1977)
     (0,1,3), # Toy Story (1995)
     (0,16,3), # Casino (1995)
     (0,25,4), # Leaving Las Vegas (1995)
     (0,32,4), # Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
     (0,296,3), # Pulp Fiction (1994)
     (0,858,5) , # Godfather, The (1972)
     (0,318,5), #Shawshank Redemption, The (1994)
     (0,33166,4), #Crash (2004)
     (0,148478,4), #Monkey King: Hero Is Back (2015)
     (0,152226,3), #Kill Game (2015)
     (0,161354,4), #Batman: The Killing Joke (2016)
     (0,67295,4), #Kung Fu Panda: Secrets of the Furious Five (2008)
     (0,67408,3), #Monsters vs. Aliens (2009)
     (0,106782,4), #Wolf of Wall Street, The (2013)
     (0,106344,3), #Starving Games, The (2013)
     (0,31267,4), #Zhou Yu's Train (Zhou Yu de huo che) (2002)
     (0,72910,4), #King of the Children (Hai zi wang) (1987)
     (0,60069,5), #WALL·E (2008)
     (0,86880,4), #Pirates of the Caribbean: On Stranger Tides (2011)
     (0,109374,4), #Grand Budapest Hotel, The (2014)
     (0,110281,5), #King of Comedy (Hei kek ji wong) (1999)
     (0,356,4), #Forrest Gump (1994)
     (0,357,4) #Four Weddings and a Funeral (1994)
    ]

rank = 5
new_user_ratings_RDD = sc.parallelize(new_user_ratings)


#add new user data to original data
new_rating = complete_rating.union(new_user_ratings_RDD)





#%%
#get the movies list of the new user data,filter the movies new user did not rated
new_rating_movies = map(lambda x: x[1], new_user_ratings)
unrated_movies = (movies_data.filter(lambda x: int(x[0]) not in new_rating_movies).map(lambda x: (new_Id, x[0])))

#setup new model from new dataset and predict unrated movies for new user
new_model = ALS.train(new_rating, rank, numIterations,lambda_ = regulz_para)
new_recomd = new_model.predictAll(unrated_movies)

new_recomd_rating = new_recomd.map(lambda x: (x.product, x.rating))
new_recomd_rating_title = new_recomd_rating.join(movies_title).join(movie_rating_counts)


new_recomd_rating_title = new_recomd_rating_title.map(lambda r: (r[0], r[1][0][1], r[1][0][0], r[1][1]))

#(movieId, movieTitle, rating,  rated movies number)
new_recomd_rating_title.take(5)

#choose top 5 movies for this user with rating number > 30
top_movies = new_recomd_rating_title.filter(lambda x: x[3]> 30)\
    .map(lambda x: (x[1],x[2])).takeOrdered(5, key=lambda x: -x[1])


print ('TOP recommended movies (with more than 30 reviews):\n%s' %
        '\n'.join(map(str, top_movies)))



























