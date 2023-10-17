from sklearn.cluster import DBSCAN
import numpy as np
from coordinate_api import get_coordinates_from_tweet_ids
from tweet_api_data import get_tweets


def cluster_coordinates(tweet_ids):
    coordinates = get_coordinates_from_tweet_ids(tweet_ids)

    # Convert coordinates to a numpy array for DBSCAN
    coordinates_array = np.array(coordinates)

    # Set the maximum distance between two samples to be considered as in the same neighborhood
    eps = 7.0  # Adjust this value based on your data and desired proximity

    # Set the minimum number of samples in a neighborhood for a data point to be considered a core point
    min_samples = (
        1  # Adjust this value based on your data and the desired minimum cluster size
    )

    # Create and fit the DBSCAN model
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    dbscan.fit(coordinates_array)

    # Get the cluster labels (clusters are indicated by positive integers, and noise is labeled as -1)
    cluster_labels = dbscan.labels_

    # Calculate cluster centroids
    from sklearn.metrics.pairwise import euclidean_distances

    unique_labels = np.unique(cluster_labels)
    cluster_centroids = {}

    for label in unique_labels:
        cluster_points = coordinates_array[cluster_labels == label]
        centroid = cluster_points.mean(axis=0)
        cluster_centroids[label] = tuple(centroid)

    return cluster_centroids
