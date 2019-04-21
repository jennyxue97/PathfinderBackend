from flask import Flask, request, jsonify
from flask_cors import CORS
import yelp, database

app = Flask(__name__)
CORS(app)

@app.route("/get_businesses")
def route_get_businesses():
    search_queries = request.args.getlist("search_queries[]")
    location = request.args["location"]
    days = int(request.args["days"])
    return jsonify(yelp.get_businesses(search_queries, location, days))

@app.route("/get_itineraries_from_user")
def route_get_itinerary_from_user():
    user_id = request.args["user_id"]
    return jsonify(database.get_itineraries_from_user(user_id))

@app.route("/get_itinerary")
def route_get_itinerary():
    user_id = request.args["user_id"]
    itinerary_id = request.args["itinerary_id"]
    return jsonify(database.get_itinerary(user_id, itinerary_id))
    
@app.route("/post_itinerary")
def route_post_itinerary():
    itinerary = request.form["itinerary"]
    database.delete(itinerary["itinerary_id"], itinerary["user_id"])
    return jsonify(database.post_itinerary(itinerary))

@app.route("/post_itinerary_in_user")
def route_post_itinerary_in_user():
    itinerary_id = request.args["itinerary_id"]
    itinerary_name = request.args["itinerary_name"]
    user_id = request.args["user_id"]
    user_name = request.args["user_name"]
    return jsonify(database.post_itinerary_in_user(user_id, user_name, itinerary_id, itinerary_name))

@app.route("/delete_itineary_from_user")
def route_delete_itinerary_from_user():
    itinerary_id = request.args["itinerary_id"]
    user_id = request.args["user_id"]
    return jsonify(database.delete_itinerary_from_user(user_id, itinerary_id))


if __name__ == "__main__":
    app.run()