import matplotlib.pyplot as plt
from yelpapi import YelpAPI
import os
from sklearn.cluster import KMeans
import numpy as np
import json

def get_businesses(search_queries, city, days):
    """
    Gets the business data to pass to the front end

    Params
        -----
        search_queries (list): list of queries that the user inputs
        location (string): name of location that user inputs
    """
    location_to_business_map = {} #{(longitude, latitude): business object}
    query_to_business_map = {} #{query: [business object]}
    query_to_location_map = {} #{query: [locations]}
    
    for query in search_queries:

        businesses = get_results(query, city)

        query_to_location_map[query] = set()
        query_to_business_map[query] = businesses

        locations = get_locations(businesses)

        for location in locations:
            if location not in location_to_business_map:
                location_to_business_map[location] = locations[location]
                query_to_location_map[query].add(location)

    kmeans_label = apply_kmeans(list(location_to_business_map.keys()), days)

    total_results = []
    for label in kmeans_label:

        # Sort by highest reviewed items first
        review_counts = []
        results = kmeans_label[label]

        for result in results:
            review_count = location_to_business_map[tuple(result)]["review_count"]
            review_counts.append(review_count)
        
        results = list(map(lambda x: x.tolist(), results))
        sorted_labels = [x for _,x in sorted(zip(review_counts, results))][::-1]


        cluster_results = {}
        for query in search_queries:
            cluster_results[query] = []

        location_set = set()
        for location in sorted_labels:
            for query in search_queries:
                if tuple(location) in query_to_location_map[query] and len(cluster_results[query]) < 3 and tuple(location) not in location_set:
                    longitude = location[0]
                    latitude = location[1]
                    updated_information = {}
                    information = location_to_business_map[tuple(location)]
                    updated_information["name"] = information["name"]
                    updated_information["url"] = information["url"]
                    updated_information["image_url"] = information["image_url"]
                    updated_information["review_count"] = information["review_count"]
                    updated_categories = []
                    for category in information["categories"]:
                        updated_categories.append(category["title"])
                    updated_information["categories"] = updated_categories
                    updated_information["rating"] = information["rating"]
                    updated_information["location"] = information["location"]["display_address"][0]
                    updated_information["phone"] = information["phone"]
                    updated_information["coordinates"] = str(longitude) + " " + str(latitude)

                    cluster_results[query].append(updated_information)
                    location_set.add(tuple(location))

        total_results.append(cluster_results)
   
    return {"clusters": total_results}     

def get_results(term, location):
    yelp_api = YelpAPI(os.environ["YELP_API_KEY"])

    results = yelp_api.search_query(term=term,
                                  location=location,
                                  limit=50,
                                  sort_by="review_count",
                                  radius=8000
    )
    
    businesses = results["businesses"]
    distance_business = []
    for business in businesses:
        if business["distance"] < 8000:
            distance_business.append(business)
    return distance_business

def get_locations(businesses):
    location_to_business_map = {}
    for business in businesses:
        coordinates = (business["coordinates"]["longitude"], business["coordinates"]["latitude"])
        location_to_business_map[coordinates] = business
    return location_to_business_map

def apply_kmeans(locations, days):
    X = np.array(list(map(lambda x: list(x), locations)))
    kmeans = KMeans(n_clusters=days, random_state=2).fit(X)
    y_kmeans = kmeans.predict(X)

    kmeans_labels = {}
    for i in range(len(kmeans.labels_)):
        label = kmeans.labels_[i]
        if label not in kmeans_labels:
            kmeans_labels[label] = []
        kmeans_labels[label].append(X[i])
    return kmeans_labels

