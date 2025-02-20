from flask import Flask, jsonify
import mysql.connector

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='David2001$$',
        database='sakila'
    )


@app.route("/top5RentedMovies", methods=['GET'])
def get_top_rented_movies():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    
    query = """
        SELECT film.title, COUNT(film.film_id) AS rental_count, film.description,
        film.rental_rate, film.release_year,film.rental_duration,
        film.length, film.replacement_cost,film.rating, 
        CONCAT (film.special_features, '') AS special_features
        FROM rental, film, inventory
        WHERE rental.inventory_id = inventory.inventory_id
        AND inventory.film_id = film.film_id
        GROUP BY film.title, film.description, film.rental_rate, film.release_year,
        film.rental_duration, film.length, film.replacement_cost, film.rating,
        film.special_features
        ORDER BY COUNT(film.film_id) DESC
        LIMIT 5;
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)

@app.route("/topActors", methods=['GET'])
def top_actors():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    
    query = """
            SELECT actor.actor_id, first_name, last_name, COUNT(film.film_id) As rental_count
            FROM actor, film_actor, film
            WHERE film.film_id = film_actor.film_id
            AND film_actor.actor_id = actor.actor_id
            GROUP BY actor_id
            ORDER BY COUNT(film.film_id) DESC
            LIMIT 5;
    """

    cursor.execute(query)
    result = cursor.fetchall()
    for actor in result:
        id = actor['actor_id']
        movies = f"""
            SELECT film.film_id, film.title, COUNT(film.film_id) AS Count
            FROM film, film_actor, rental, inventory
            WHERE film.film_id = film_actor.film_id
            AND inventory.film_id = film.film_id
            AND rental.inventory_id = inventory.inventory_id
            AND film_actor.actor_id = {id}
            GROUP BY film.film_id
            ORDER BY COUNT(film.film_id) DESC
            LIMIT 5;
            """
        
        cursor.execute(movies)
        result2 = cursor.fetchall()
        actor["top_movie"] = result2



    cursor.close()
    db.close()
    return jsonify(result)

@app.route("/Allfilms", methods=['GET'])
def getmovies():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    
    query = """
        SELECT *, CONCAT (film.special_features, '') AS special_features,
        CONCAT (film.last_update, '') AS last_update
        FROM film;
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)

@app.route("/Allcustomers", methods=['GET'])
def getcustomers():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    
    query = """
            SELECT customer.customer_id, customer.first_name, customer.last_name, COUNT(rental.rental_id) AS Count,
            customer.email, customer.active,
            CONCAT (customer.create_date, '') AS create_Date
            FROM customer, rental
            WHERE customer.customer_id = rental.customer_id
            GROUP BY customer.customer_id
            ORDER BY COUNT(rental.rental_id) DESC;
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
