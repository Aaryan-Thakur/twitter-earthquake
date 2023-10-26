from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
uri = "mongodb+srv://aaryan:tFfglQNdgX9uP0UH@cluster0.cug9lgv.mongodb.net/?retryWrites=true&w=majority"

def get_tweets():
    tweet_ids = []  
    try:
        client = MongoClient(uri)
        db = client.get_database('main') 
        collection = db['tweets']
        all_tweets = collection.find()
        for tweet in all_tweets:
            if(tweet['y']=='s'):
                tweet_ids.append(tweet['tid'])
        return tweet_ids
    except Exception as e:
        print(e)
        return[]


uri = "mongodb+srv://aaryan:tFfglQNdgX9uP0UH@cluster0.cug9lgv.mongodb.net/?retryWrites=true&w=majority"
def get_coordinates(tweet_ids):
    result = []
    try:
        client = MongoClient(uri)
        db = client.get_database('main') 
        locations = db['location']
        cords = locations.find({'tid': {'$in': tweet_ids}})
        for cord in cords:
            data = (cord['latitude'],cord['longitude'])
            result.append(data)
        return result
    except Exception as e:
        print(e)
        return[]