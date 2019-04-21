from flask import Flask, request, jsonify
from flask_cors import CORS
import yelp, database
import json

app = Flask(__name__)
CORS(app)

@app.route("/get_businesses", methods=["GET"])
def route_get_businesses():
    search_queries = request.args.getlist("search_queries[]")
    location = request.args["location"]
    days = int(request.args["days"])
    return jsonify(yelp.get_businesses(search_queries, location, days))

@app.route("/get_itineraries_from_user", methods=["GET"])
def route_get_itinerary_from_user():
    user_id = request.args["user_id"]
    return jsonify(database.get_itineraries_from_user(user_id))

@app.route("/get_itinerary", methods=["GET"])
def route_get_itinerary():
    user_id = request.args["user_id"]
    itinerary_id = request.args["itinerary_id"]
    return jsonify(database.get_itinerary(user_id, itinerary_id))
    
@app.route("/post_itinerary", methods=["POST"])
def route_post_itinerary():
    itinerary_id = request.form["itinerary_id"]
    itinerary_name = request.form["itinerary_name"]
    user_id = request.form["user_id"]
    user_name = request.form["user_name"]
    itinerary = json.loads(request.form["itinerary"])
    database.delete_itinerary(user_id, itinerary_id)
    return jsonify(database.post_itinerary(user_name, user_id, itinerary_name, itinerary_id, itinerary))

@app.route("/post_itinerary_in_user", methods=["POST"])
def route_post_itinerary_in_user():
    itinerary_name = request.form["itinerary_name"]
    user_id = request.form["user_id"]
    user_name = request.form["user_name"]
    return jsonify(database.post_itinerary_in_user(user_id, user_name, itinerary_name))

@app.route("/delete_itinerary_from_user", methods=["POST"])
def route_delete_itinerary_from_user():
    itinerary_id = request.form["itinerary_id"]
    user_id = request.form["user_id"]
    return jsonify(database.delete_itinerary_from_user(user_id, itinerary_id))


if __name__ == "__main__":
    app.run()