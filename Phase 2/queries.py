# queries.py

queries = {
    0: {
        'description': '1) Listings with a certain number of reviews in a specific price range',
        'sql': """
            SELECT l.name, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price, ROUND(l.reviews_per_month, 2) AS reviews_per_month
            FROM Listings l
            WHERE l.price BETWEEN :lowest_value AND :highest_value
              AND l.number_of_reviews >= :min_reviews
            ORDER BY l.reviews_per_month DESC;
        """,
        'params': [
            ('lowest_value', 'Enter lowest price:', float),
            ('highest_value', 'Enter highest price:', float),
            ('min_reviews', 'Enter minimum number of reviews:', int)
        ]
    },
    1: {
        'description': '2) Count of listings in each neighbourhood for a given price range and availability',
        'sql': """
            SELECT l.neighbourhood, COUNT(l.name) AS listings_count
            FROM Listings l
            WHERE l.price BETWEEN :min_price AND :max_price
              AND l.availability_365 >= :min_availability
            GROUP BY l.neighbourhood
            ORDER BY listings_count DESC;
        """,
        'params': [
            ('min_price', 'Enter minimum price:', float),
            ('max_price', 'Enter maximum price:', float),
            ('min_availability', 'Enter minimum availability (days per year):', int)
        ]
    },
    2: {
        'description': '3) Average price of listings in each neighbourhood, ordered by the number of reviews',
        'sql': """
            SELECT l.neighbourhood, 'R' || TO_CHAR(AVG(l.price), 'FM999,999,999.00') AS average_price, SUM(l.number_of_reviews) AS total_reviews
            FROM Listings l
            GROUP BY l.neighbourhood
            ORDER BY total_reviews DESC;
        """,
        'params': []
    },
    3: {
        'description': '4) Listings with the most reviews in a specific price range',
        'sql': """
            SELECT l.name, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price, l.number_of_reviews
            FROM Listings l
            WHERE l.price BETWEEN :lowest_value AND :highest_value
            ORDER BY l.number_of_reviews DESC
            LIMIT 10;
        """,
        'params': [
            ('lowest_value', 'Enter lowest price:', float),
            ('highest_value', 'Enter highest price:', float)
        ]
    },
    4: {
        'description': '5) Neighbourhoods with highest availability and average price',
        'sql': """
            SELECT l.neighbourhood, ROUND(AVG(l.availability_365), 2) AS avg_availability, 'R' || TO_CHAR(AVG(l.price), 'FM999,999,999.00') AS avg_price
            FROM Listings l
            GROUP BY l.neighbourhood
            ORDER BY avg_availability DESC
            LIMIT 10;
        """,
        'params': []
    },
    5: {
        'description': '6) Neighbourhoods with most listings under a specific price per night',
        'sql': """
            SELECT l.neighbourhood, COUNT(l.name) AS listings_count
            FROM Listings l
            WHERE l.price <= :max_price
            GROUP BY l.neighbourhood
            ORDER BY listings_count DESC;
        """,
        'params': [
            ('max_price', 'Enter maximum price per night:', float)
        ]
    },
    6: {
        'description': '7) Listings for groups with positive feedback below city average price',
        'sql': """
            WITH city_avg AS (
                SELECT AVG(price) AS average_price
                FROM Listings
            )
            SELECT l.name, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price, l.number_of_reviews
            FROM Listings l, city_avg
            WHERE l.minimum_nights >= :group_size
              AND l.price <= city_avg.average_price
              AND l.number_of_reviews >= :min_reviews
            ORDER BY l.number_of_reviews DESC;
        """,
        'params': [
            ('group_size', 'Enter minimum group size:', int),
            ('min_reviews', 'Enter minimum number of reviews:', int)
        ]
    },
    7: {
        'description': '8) Room Type Analysis: Reviews and Average Price',
        'sql': """
            SELECT l.room_type, SUM(l.number_of_reviews) AS total_reviews, 'R' || TO_CHAR(AVG(l.price), 'FM999,999,999.00') AS avg_price
            FROM Listings l
            GROUP BY l.room_type
            ORDER BY total_reviews DESC;
        """,
        'params': []
    },
    8: {
        'description': '9) Relationship between price and popularity in popular neighbourhoods',
        'sql': """
            SELECT l.neighbourhood, 'R' || TO_CHAR(AVG(l.price), 'FM999,999,999.00') AS avg_price, ROUND(AVG(l.reviews_per_month), 2) AS avg_reviews_per_month
            FROM Listings l
            WHERE l.price <= :max_price
              AND l.reviews_per_month IS NOT NULL
              AND l.price IS NOT NULL
            GROUP BY l.neighbourhood
            ORDER BY avg_reviews_per_month DESC;
        """,
        'params': [
            ('max_price', 'Enter maximum price per night:', float)
        ]
    },
    9: {
        'description': '10) Best value listings in under-serviced areas',
        'sql': """
            SELECT l.name, l.neighbourhood, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price, l.number_of_reviews, l.availability_365
            FROM Listings l
            WHERE l.price <= :max_price
              AND l.availability_365 < :max_availability
            ORDER BY l.number_of_reviews DESC, l.price ASC;
        """,
        'params': [
            ('max_price', 'Enter maximum price per night:', float),
            ('max_availability', 'Enter maximum availability (days per year):', int)
        ]
    },
    10: {
        'description': '11) Listings sorted from highest to lowest price',
        'sql': """
            SELECT l.name, l.neighbourhood, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price
            FROM Listings l
            WHERE l.price IS NOT NULL
            ORDER BY l.price DESC;
        """,
        'params': []
    },
    11: {
        'description': '12) Listings sorted from lowest to highest price',
        'sql': """
            SELECT l.name, l.neighbourhood, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price
            FROM Listings l
            WHERE l.price IS NOT NULL
            ORDER BY l.price ASC;
        """,
        'params': []
    },
    12: {
        'description': '13) Listings with a minimum amount of reviews',
        'sql': """
            SELECT l.name, l.number_of_reviews
            FROM Listings l
            WHERE l.number_of_reviews >= :min_reviews
            ORDER BY l.number_of_reviews DESC;
        """,
        'params': [
            ('min_reviews', 'Enter minimum number of reviews:', int)
        ]
    },
    13: {
        'description': '14) Listings by specific price and number of reviews',
        'sql': """
            SELECT l.name, 'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price, l.number_of_reviews
            FROM Listings l
            WHERE l.price <= :max_price
              AND l.number_of_reviews >= :min_reviews
            ORDER BY l.price DESC, l.number_of_reviews DESC;
        """,
        'params': [
            ('max_price', 'Enter maximum price per night:', float),
            ('min_reviews', 'Enter minimum number of reviews:', int)
        ]
    },
    14: {
        'description': '15) Likelihood of a listing being available',
        'sql': """
            SELECT l.name, l.availability_365, ROUND((l.availability_365 / 365.0) * 100, 2) AS availability_percentage
            FROM Listings l
            ORDER BY availability_percentage DESC;
        """,
        'params': []
    },
    15: {  # Question 16
        'description': '16) Which Airbnb listings have the highest rating?',
        'sql': """
            SELECT l.name, ROUND(l.rating, 2) AS rating
            FROM Listings l
            WHERE l.rating IS NOT NULL
            ORDER BY l.rating DESC
            LIMIT 10;
        """,
        'params': []
    },
    16: {  # Question 17
        'description': '17) Which Airbnb listings have the lowest rating?',
        'sql': """
            SELECT l.name, ROUND(l.rating, 2) AS rating
            FROM Listings l
            WHERE l.rating IS NOT NULL
            ORDER BY l.rating ASC
            LIMIT 10;
        """,
        'params': []
    },
    17: {  # Question 18
        'description': '18) What are the listings based on room type?',
        'sql': """
            SELECT l.room_type, COUNT(l.name) AS listings_count
            FROM Listings l
            GROUP BY l.room_type
            ORDER BY listings_count DESC;
        """,
        'params': []
    },
    # Add additional queries here...
}
