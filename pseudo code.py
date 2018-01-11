reader = read(rating.csv)
complete_rating = read(userId, movieId, rating) from reader

reader = read(movie.csv)
movie_id_title = read(movieId, title) from reader

#generate 5 pairs of validation and train datasets
validate1, validate2, validate3, validate4, validate5 = complete_rating.randomSplit([0.2,0.2,0.2,0.2,0.2])
train1 = complete_rating.subtract(validate1)
train2 = complete_rating.subtract(validate2)
train3 = complete_rating.subtract(validate3)
train4 = complete_rating.subtract(validate4)
train5 = complete_rating.subtract(validate5)

def counts_and_averge(rating_tuple):
	nrating = calculate rating numbers for a movie
	return movieId, nrating, average_movie_score

#aggregate number of movies by movidID 	
movie_ID_rating = complete_rating.groupByKey(movieId)
movie_ID_avg_rating = movie_ID_rating.map(counts_and_averages).map(movieId, movieNumber)


# calculate movie orders for an user by his/her rating scores
def order_movies(ratings_tuple):
    movie_order = sorted(ratings_tuple) reverserly by rating scores
    for seq, movie_rating in enumerate(movie_order):
        temp = (movieId, seq)
        movie_sequence = list of pairs of (movieId and index) of rated movies for an user
    return movieId,movie_seq

# get list of predict index and actual index for rated movies of an user
def movie_index(movie_ratings_tuple):
    dict0 = dict(movie_ratings_tuple[0])
    dict1 = dict(movie_ratings_tuple[1])
    temp = join predict index and actual index by key movieId
    l1 = list()
    l2 = list()
    for i in temp:
        l1.append(predicted index)
        l2.append(actual index)
        if len(l1) > 10: #choose top 1~10 movies for an user
            break
    return (l1,l2)
    

def bestmodel(traindata, validatedata):
	bestValidationRmse = float('f')
    validation = ap validate data to userId, movieId
    ratingTuple = validatedata.map(get actual rating data for pairs of userId, movieId)
    for rank in ranks:
        model = creat model (ALS.train(traindata, rank, numIterations,lambda_ = regulz_para))

        predictions = model.predictAll(predict ratings for userId, movieId in a validation dataset)      

        scoreAndLabels = predictions.join(ratingTuple).map(create tuples contain predict and actual ratings)
        
        
        regMetrics = RegressionMetrics(scoreAndLabels)
        RMSE = regMetrics.rootMeanSquaredError
        MSE = regMetrics.meanSquaredError
        print("For rank %s:" % rank)
        print("RMSE = %s" % RMSE)
        print("MSE = %s" % MSE)
        
        
        if RMSE < bestValidationRmse: #compared different rank values by RMSE 
            bestValidationRmse = RMSE 
            best_rank = rank
               
    print 'The best model was trained with rank %s' % best_rank
    
    #calculate MAP: 
    model = creat best model by best_rank(ALS.train(traindata, best_rank, numIterations,lambda_ = regulz_para))
    actual_user_movie = validatedata.map(lambda x: (userId, (movieId, rating))).groupByKey(userId)
    actual_user_movie_sequence = actual_user_movie.map(order_movies) 
     
    predict_user_movie = model.predictAll(validation).map(lambda r: (userId, (movieId, rating))).groupByKey(userId)
    predict_user_movie_sequence = predict_user_movie.map(order_movies) 
    
    movie_seq = predict_user_movie1.join(predict_user_movie_sequence).map(movieId)
    movie_seq = movie_seq.map(movie_index) #generate predict sequence and actual sequence for the movies of an user
    
    rankMetrics = RankingMetrics(movie_seq)
    MAP = rankMetrics.meanAveragePrecision
    print("MAP = %s" % MAP) 
    
#5 folds cross validataion 
bestmodel(train1, validate1)
bestmodel(train2, validate2) 
bestmodel(train3, validate3) 
bestmodel(train4, validate4)        
bestmodel(train5, validate5)         
        
        

#%% insert new user
new_Id = 0
new_user_ratings = new rating records of a new user (userId, movieId, rating)
new_rating = old_rating_data.union(new_user_ratings_RDD)

new_rating_movies = map(rated movies of the new user)
unrated_movies = (movies_data.filter(unrated movies of the new user).map(lambda x: (new_Id, movieId)))

new_model = create new ALS model for new rating records
new_recomd = new_model.predictAll(unrated_movies)

new_recomd_rating = new_recomd.map(lambda x: (x.product, x.rating))
new_recomd_rating_title = new_recomd_rating.join(movies_title and movie_rating_counts) 

new_recomd_rating_title = new_recomd_rating_title.map(lambda r: (movieId, movieTitle, rating, counts))


#choose top 5 movies for this user with rating number > 30
top_movies = new_recomd_rating_title.filter(rating number >30)\
    .map(movieId, movieTitle).takeOrdered(5 by rating scores)


print ('TOP recommended movies (with more than 30 reviews):\n%s' %
        '\n'.join(map(str, top_movies)))


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    