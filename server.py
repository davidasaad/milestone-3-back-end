from flask import Flask, jsonify, request
import mysql.connector

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='1234',
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
    for id in result:
        id2 = id['film_id']
        copies = f"""
                SELECT inventory.film_id, 
                    COUNT(inventory.inventory_id) AS Total_Copies,
                    COUNT(inventory.inventory_id) - 
                    (SELECT COUNT(*) FROM rental 
                        WHERE rental.inventory_id IN 
                            (SELECT inventory_id FROM inventory WHERE film_id = {id2}) 
                            AND rental.return_date IS NULL) AS DVD_Copies
                FROM inventory
                WHERE inventory.film_id = {id2}
                GROUP BY inventory.film_id;

            """
        
        cursor.execute(copies)
        result3 = cursor.fetchall()
        id["Copies"] = result3



    cursor.close()
    db.close()
    return jsonify(result)

@app.route("/Allcustomers", methods=['GET', 'POST'])
def getcustomers():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    if request.method == 'GET':
        query = """
                SELECT customer.customer_id, customer.first_name, customer.last_name, COUNT(rental.rental_id) AS Count,
                    customer.email,
                    COUNT(CASE WHEN rental.return_date IS NULL AND rental.rental_date IS NOT NULL THEN 1 END) AS active_rentals,
                    CONCAT (customer.create_date, '') AS create_Date,
                    CONCAT (customer.last_update, '') AS update_Date,
                    address.address, city.city, country.country, address.district, address.postal_code, address.phone
                FROM customer
                JOIN address ON customer.address_id = address.address_id
                JOIN city ON address.city_id = city.city_id
                JOIN country ON city.country_id = country.country_id
                LEFT JOIN rental ON customer.customer_id = rental.customer_id
                GROUP BY customer.customer_id
                ORDER BY create_date DESC;
        """

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        db.close()
        return jsonify(result)
    
    elif request.method == 'POST':
        data =request.json
        customer_id = data.get("customer_id")
        first_name = data.get("first_name")
        last_name = data.get("last_name")
        email = data.get("email")
        address = data.get("address")
        city = data.get("city")
        district = data.get("district")
        postal_code = data.get("postal_code")
        phone = data.get("phone")
        
        query = """ 
                UPDATE customer
                SET first_name = %s, last_name = %s, email = %s
                WHERE customer_id = %s
                     """
        cursor.execute(query, (first_name, last_name, email, customer_id))

        query1 = """
                SELECT address_id FROM customer WHERE customer_id = %s LIMIT 1 
            """
        cursor.execute(query1, (customer_id, ))
        address_get = cursor.fetchone()
        address_id = address_get["address_id"]

        query2 = """
                UPDATE address
                SET address = %s, district = %s, postal_code = %s, phone = %s, last_update = NOW()
                WHERE address_id = %s
            """
        cursor.execute(query2, (address, district, postal_code, phone, address_id))

        query3 =" SELECT city_id FROM address WHERE address_id = %s LIMIT 1"
        cursor.execute(query3, (address_id, ))
        city_get = cursor.fetchone()
        city_id = city_get["city_id"]

        query4 = """
                UPDATE city
                SET city = %s
                WHERE city_id = %s
                    """
        cursor.execute(query4, (city, city_id))
        db.commit()
        cursor.close()
        db.close()
        return jsonify({"message" : "Updated Successfully"})        
            
@app.route("/newRental", methods=['POST'])
def newRental():
    data = request.json
    customer_id = data.get("customer_id")
    film_id = data.get("film_id")

    if not customer_id or not film_id:
        return jsonify({"error": "customer id and film id are required"}), 400

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    query_customer = "SELECT customer_id FROM customer WHERE customer_id = %s LIMIT 1"
    cursor.execute(query_customer, (customer_id,))
    customer_check = cursor.fetchone()

    if not customer_check:
        return jsonify({"error": "Customer does not exist"}), 400

    query0 = "SELECT inventory_id FROM inventory WHERE film_id = %s LIMIT 1"
    cursor.execute(query0, (film_id,))
    inventory_check = cursor.fetchone()


    if not inventory_check:
        return jsonify({"error":"No available copies for this movie"}), 400

    inventory_id = inventory_check["inventory_id"]


    query = """
            INSERT INTO rental (rental_date, inventory_id, customer_id, last_update, return_date, staff_id)
            VALUES (NOW(), %s, %s, NOW(), NULL, 1)
        """
    cursor.execute(query, (inventory_id, customer_id))

    db.commit()
    cursor.close()
    db.close()
    return jsonify({"message": "Rental successfully added"}), 201


@app.route("/deletecustomer", methods=['DELETE'])
def deletecustomer():
    data = request.json
    customer_id = data.get("customer_id")

    
    if not customer_id:
        return jsonify({"error": "Customer ID is required"}), 400
    
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    query0 = "DELETE FROM rental WHERE customer_id = %s"
    cursor.execute(query0, (customer_id,))

    query = "DELEtE FROM customer WHERE customer_id = %s"
    cursor.execute(query, (customer_id, ))

    db.commit()
    cursor.close()
    db.close()
    
    return jsonify({"message": "Customer deleted successfully"}), 200


@app.route("/addcustomer", methods=['POST'])
def addcustomer():
    data = request.json
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    email = data.get("email")
    address = data.get("address")
    city = data.get("city")
    country = data.get("country")
    district = data.get("district")
    postal_code = data.get("postal_code")
    phone = data.get("phone")

    if not all([first_name, last_name, email, address, city, country, district, phone]):
        return jsonify({"error": "All are required"}), 400


    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    query0 = "SELECT country_id FROM country WHERE country = %s LIMIT 1"
    cursor.execute(query0, (country,))
    country_check = cursor.fetchone()

    if (len(country_check) != 0):
        country_id = country_check["country_id"]

    else:
        query0_1 = "INSERT INTO country(country, last_update) VALUES (%s, NOW())"
        cursor.execute(query0_1, (country,))
        query0_2 = "SELECT country_id FROM country WHERE country = %s LIMIT 1"
        cursor.execute(query0_2, (country,))
        country_hold = cursor.fetchone()
        country_id = country_hold["country_id"]



    query1 = "INSERT INTO city (city, country_id, last_update) VALUES (%s, %s, NOW())"
    cursor.execute(query1, (city, country_id))
    query1_1 = "SELECT city_id FROM city WHERE city = %s LIMIT 1"
    cursor.execute(query1_1, (city, ))
    city_hold = cursor.fetchone()
    city_id = city_hold["city_id"]


    query2_0 = "SELECT location FROM address LIMIT 1"
    cursor.execute(query2_0)
    location_hold = cursor.fetchone()
    location = location_hold["location"]

    query2 = """INSERT INTO address (address, address2, district, city_id, postal_code, phone, last_update, location) 
                VALUES (%s, NULL, %s, %s, %s, %s, NOW(), %s)
    """
    cursor.execute(query2, (address, district, city_id, postal_code, phone, location))
    query2_1 = "SELECT address_id FROM address WHERE city_id = %s AND address = %s"
    cursor.execute(query2_1, (city_id, address))
    address_hold = cursor.fetchone()
    address_id = address_hold["address_id"]



    query = """INSERT INTO customer (first_name, last_name, email, store_id, address_id, active, create_date, last_update)
                                    VALUES (%s, %s, %s, 1, %s, 1, NOW(), NOW())"""
    cursor.execute(query, (first_name, last_name, email, address_id))

    db.commit()
    cursor.close()
    db.close()
    
    return jsonify({"message": "Customer added successfully"}), 200


@app.route("/returnrental", methods=['POST'])
def returnRental():
    data = request.json
    customer_id = data.get('customer_id')
    film_id = data.get('film_id')
    if not film_id:
        return jsonify({"error": "Film ID is required"}), 400
    
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    query1 = """SELECT rental_id
                FROM rental, inventory
                WHERE return_date IS NULL
                AND customer_id = %s
                AND rental.inventory_id = inventory.inventory_id
                AND inventory.film_id = %s
    
    """
    cursor.execute(query1, (customer_id, film_id))
    rental_record = cursor.fetchone()
    if not rental_record:
        cursor.close()
        db.close()
        return jsonify({"error": "No active rental found to return"}), 400

    rental_id = rental_record["rental_id"]


    query2 = """UPDATE rental
                SET return_date = NOW()
                WHERE rental_id = %s
    """ 
    cursor.execute(query2, (rental_id,))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"message": f"Film {film_id} successfully returned."})

if __name__ == "__main__":
    app.run(debug=True)
