from google.cloud import bigquery
import pandas as pd

def post_itinerary(itinerary):
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    itinerary_id = itinerary["itinerary_id"]
    itinerary_name = itinerary["itinerary_name"]
    user_id = itinerary["user_id"]
    user_name = itinerary["user_name"]
    for cluster_id, cluster in enumerate(itinerary["clusters"]):
        for query in cluster:
            businesses = cluster[query]
            for order, business in enumerate(businesses):
                business_name = business["name"]
                url = business["url"]
                image_url = business["image_url"]
                review_count = business["review_count"]
                categories = [None, None, None]
                for idx, category in enumerate(business["categories"]):
                    categories[idx] = category
                rating = business["rating"]
                location = business["location"]
                phone = business["phone"]
                coordinates = business["coordinates"]
                
                sql_query = """
                    INSERT INTO
                        `pathfinder-1555643805221.Pathfinder.Itineraries` (
                            user_id,
                            user_name,
                            business_name,
                            url,
                            image_url,
                            review_count,
                            category_1,
                            category_2,
                            category_3,
                            rating,
                            location,
                            phone,
                            itinerary_id,
                            itinerary_name,
                            cluster_id,
                            query,
                            coordinates,
                            `order`
                        )
                    VALUES ("%s",  
                            "%s",
                            "%s",
                            "%s",
                            "%s",
                            %s,
                            "%s",
                            "%s",
                            "%s",
                            %s,
                            "%s",
                            "%s",
                            "%s",
                            "%s",
                            %s,
                            "%s",
                            "%s",
                            %s)
                    """%(user_id, user_name, business_name, url, image_url, review_count, 
                        categories[0], categories[1], categories[2], rating, location, phone, 
                        itinerary_id, itinerary_name, cluster_id, query, coordinates, order)
                # print(sql_query)
                query_job = client.query(sql_query)
                results = query_job.result()

    return results

def delete_itinerary(user_id, itinerary_id):
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    query = """
    DELETE FROM
        `pathfinder-1555643805221.Pathfinder.Itineraries`
    WHERE
        itinerary_id="%s" AND 
        user_id="%s"
    """ %(itinerary_id, user_id)
    query_job = client.query(query)
    results = query_job.result()
    return results

def get_itinerary(itinerary_id, user_id):
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    query = """
    SELECT 
        *
    FROM
        `pathfinder-1555643805221.Pathfinder.Itineraries`
    WHERE
        itinerary_id='%s' AND
        user_id='%s'
    ORDER BY
        `order`
    """%(itinerary_id, user_id)
    query_job = client.query(query)
    results = query_job.result()
    
    parsed_results = {"clusters": []}
    clusters = {}
    df = results.to_dataframe()
    for idx, row in df.iterrows():
        results_keys = ["itinerary_id", "itinerary_name", "user_name", "user_id"]
        for results_key in results_keys:
            if results_key not in parsed_results:
                parsed_results[results_key] = row[results_key]
        

        if row["cluster_id"] not in clusters:
            clusters[row["cluster_id"]] = {}
        
        query = row["query"]
        
        if query not in clusters[row["cluster_id"]]:
            clusters[row["cluster_id"]][query] = []

        categories = []
        for i in range(1, 4):
            if row["category_" + str(i)] != "None":
                categories.append(row["category_" + str(i)])
        
        query_dict = {
            "categories": categories,
        }

        columns = ["coordinates", "image_url", "url", "location", "phone", "rating", "review_count"]
        for column in columns:
            query_dict[column] = row[column]
        query_dict["name"] = row["business_name"]
        
        clusters[row["cluster_id"]][query].append(query_dict)

    
    for idx in sorted(clusters.keys()):
        parsed_results["clusters"].append(clusters[idx])

    return parsed_results

def post_itinerary_in_user(user_id, user_name, itinerary_id, itinerary_name):
    if is_duplicate_itinerary(user_id, itinerary_id, itinerary_name):
        return False
    
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    query = """
    INSERT INTO
        `pathfinder-1555643805221.Pathfinder.Users` (user_id,
            user_name,
            itinerary_id,
            itinerary_name)
        VALUES (
            "%s",
            "%s",
            "%s",
            "%s"
        )
    """ %(user_id, user_name, itinerary_id, itinerary_name)
    query_job = client.query(query)
    results = query_job.result()
    return True
    

def is_duplicate_itinerary(user_id, itinerary_id, itinerary_name):
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    query = """
    SELECT 
        *
    FROM
        `pathfinder-1555643805221.Pathfinder.Users`
    WHERE
        user_id="%s" AND 
        (
        itinerary_id="%s" OR
        itinerary_name="%s"
        )
    """ %(user_id, itinerary_id, itinerary_name)
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    if len(df) > 0:
        return True
    return False
    
def get_itineraries_from_user(user_id):
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    query = """
    SELECT 
        *
    FROM
        `pathfinder-1555643805221.Pathfinder.Users`
    WHERE
        user_id="%s"
    """ %(user_id)
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()

    users = {"itineraries": {}}
    for idx, row in df.iterrows():
        if row["user_id"] not in users:
            users["user_id"] = row["user_id"]
        if row["user_name"] not in users:
            users["user_name"] = row["user_name"]
        users["itineraries"][row["itinerary_id"]] = row["itinerary_name"]

    return users

def delete_itinerary_from_user(user_id, itinerary_id):
    delete_itinerary(itinerary_id, user_id)
    client = bigquery.Client.from_service_account_json("/Users/jennyxue/Desktop/PathfinderBackend/Pathfinder-677fd1d11a8b.json")
    query = """
    DELETE FROM
        `pathfinder-1555643805221.Pathfinder.Users`
    WHERE
        itinerary_id="%s" AND 
        user_id="%s"
    """ %(itinerary_id, user_id)
    query_job = client.query(query)
    results = query_job.result()
    return results