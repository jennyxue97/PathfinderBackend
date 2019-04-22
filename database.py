from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import pandas_gbq as pdg

def post_itinerary(user_id, user_name, itinerary_name, itinerary_id, itinerary):
    columns = ["user_id", "user_name", "business_name", "url", "image_url", "review_count", 
                               "category_1", "category_2", "category_3", "rating", "location", "phone",
                               "itinerary_id", "itinerary_name", "cluster_id", "query",
                               "coordinates", "sort_order"]
    df = []
    
    itinerary_id = itinerary_name
    itinerary_name = itinerary_name
    # client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    for cluster_id, cluster in enumerate(itinerary["clusters"]):
        for query in cluster:
            businesses = cluster[query]
            for order, business in enumerate(businesses):
                business_name =  business["name"]
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
                row = [user_id, user_name, business_name, url, image_url, review_count, 
                        categories[0], categories[1], categories[2], rating, location, phone, 
                        itinerary_id, itinerary_name, cluster_id, query, coordinates, order]
                df.append(row)

    dtypes = ["str", "str", "str", "str", "str", "int", "str", "str", "str",
              "float", "str", "str", "str", "str", "int", "str", "str", "int"]
    schemas = ["STRING", "STRING", "STRING", "STRING", "STRING", "INTEGER", "STRING", "STRING", "STRING",
               "FLOAT", "STRING", "STRING", "STRING", "STRING", "INTEGER", "STRING", "STRING", "INTEGER"]

    df = pd.DataFrame(df, columns=columns)
    table_schemas = []
    for idx, schema in enumerate(schemas):
        table_schemas.append({"name": columns[idx], "type": schema})
        df[columns[idx]] = df[columns[idx]].astype(dtypes[idx])
    credentials = Credentials.from_service_account_file("Sherpa-3244e874fcf9.json")
    pdg.to_gbq(df, "Sherpa.Itineraries", project_id="sherpa-238315", chunksize=None, if_exists="append", credentials=credentials, table_schema=table_schemas)
    return {"done": True}

def delete_itinerary(user_id, itinerary_id):
    client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    query = """
    DELETE FROM
        `sherpa-238315.Sherpa.Itineraries`
    WHERE
        itinerary_id="%s" AND 
        user_id="%s"
    """ %(itinerary_id, user_id)
    query_job = client.query(query)
    results = query_job.result()
    return results

def get_itinerary(user_id, itinerary_id):
    client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    query = """
    SELECT 
        *
    FROM
        `sherpa-238315.Sherpa.Itineraries`
    WHERE
        itinerary_id='%s' AND
        user_id='%s'
    ORDER BY
        sort_order
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

def post_itinerary_in_user(user_id, user_name, itinerary_name):
    itinerary_id = itinerary_name
    if is_duplicate_itinerary(user_id, itinerary_id, itinerary_name):
        return {"done": False, "itinerary_id": itinerary_id, "itinerary_name": itinerary_name}
    
    client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    query = """
    INSERT INTO
        `sherpa-238315.Sherpa.Users` (user_id,
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
    return {"done": True, "itinerary_id": itinerary_id, "itinerary_name": itinerary_name}
    

def is_duplicate_itinerary(user_id, itinerary_id, itinerary_name):
    client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    query = """
    SELECT 
        *
    FROM
        `sherpa-238315.Sherpa.Users`
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
    if df.shape[0] == 0:
        return False
    return True
    
def get_itineraries_from_user(user_id):
    client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    query = """
    SELECT 
        *
    FROM
        `sherpa-238315.Sherpa.Users`
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
    delete_itinerary(user_id, itinerary_id)
    client = bigquery.Client.from_service_account_json("Sherpa-3244e874fcf9.json")
    query = """
    DELETE FROM
        `sherpa-238315.Sherpa.Users`
    WHERE
        itinerary_id="%s" AND 
        user_id="%s"
    """ %(itinerary_id, user_id)
    query_job = client.query(query)
    results = query_job.result()
    return {"done": True}
