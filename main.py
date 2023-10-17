from tweet_ids_builder import get_tweets
from tweet_api_data import get_coordinates
from clustering import cluster_coordinates
from get_location_from_coordinate import location

relevant_tweet_ids = get_tweets()

coordinates_of_tweet_ids = get_coordinates(relevant_tweet_ids)

# Performing clustering of coordinates with the help of DBSCAN
centroids = cluster_coordinates(coordinates_of_tweet_ids)
print("Total number of tweets given : 300")
print(f"Number of locations with probable earthquakes found (eps=7) {len(centroids)}:")
for label, centroid in centroids.items():
    print(
        f"Provided tweet data suggests earthquake occured in : {location(round(centroid[0],2),round(centroid[1],2))}"
    )
