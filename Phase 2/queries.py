# queries.py

queries = {
    1: {
        'description': 'Top 10 most affordable Airbnb listings that accommodate at least X people, offer at least Y key amenities, and are priced under ZAR X per night.',
        'sql': """
            SELECT 
                name, 
                neighbourhood, 
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                accommodates,
                array_length(string_to_array(amenities, ','), 1) AS amenities_count
            FROM 
                Listings
            WHERE 
                accommodates >= :min_guests
                AND array_length(string_to_array(amenities, ','), 1) >= :min_amenities
                AND price <= :max_price
            {order_by_clause}
            LIMIT 10;
        """,
        'params': [
            ('min_guests', 'Enter minimum number of guests:', int),
            ('min_amenities', 'Enter minimum number of key amenities:', int),
            ('max_price', 'Enter maximum price per night (ZAR):', float)
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'accommodates', 'amenities_count']
    },
    2: {
        'description': 'Listings for groups of X or more people with highest guest ratings within a specified price range.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                review_scores_rating,
                accommodates
            FROM 
                Listings
            WHERE 
                accommodates >= :min_guests
                AND price BETWEEN :min_price AND :max_price
                AND review_scores_rating IS NOT NULL
            {order_by_clause}
            LIMIT 10;
        """,
        'params': [
            ('min_guests', 'Enter minimum number of guests:', int),
            ('min_price', 'Enter minimum price per night (ZAR):', float),
            ('max_price', 'Enter maximum price per night (ZAR):', float)
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'accommodates']
    },
    3: {
        'description': 'Neighbourhoods with the highest concentration of budget-friendly listings that accommodate X to Y people and have positive ratings.',
        'sql': """
            SELECT 
                l.neighbourhood,
                COUNT(*) AS budget_friendly_listings
            FROM 
                Listings l,
                average_price ap
            WHERE 
                l.accommodates BETWEEN :min_guests AND :max_guests
                AND l.price < ap.avg_price
                AND l.review_scores_rating > :rating_threshold
            GROUP BY 
                l.neighbourhood
            {order_by_clause};
        """,
        'params': [
            ('min_guests', 'Enter minimum number of guests:', int),
            ('max_guests', 'Enter maximum number of guests:', int),
            ('rating_threshold', 'Enter minimum review score rating (e.g., 3):', float)
        ],
        'sortable_columns': ['neighbourhood', 'budget_friendly_listings']
    },
    4: {
        'description': 'Average nightly price for listings in each neighbourhood that can accommodate a group.',
        'sql': """
            SELECT 
                neighbourhood,
                ROUND(AVG(price), 2) AS average_nightly_price
            FROM 
                Listings
            WHERE 
                accommodates >= :group_size
            GROUP BY 
                neighbourhood
            {order_by_clause};
        """,
        'params': [
            ('group_size', 'Enter group size (number of people):', int)
        ],
        'sortable_columns': ['neighbourhood', 'average_nightly_price']
    },
    5: {
        'description': 'Best value listings based on price per person per night for groups of X people.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                accommodates,
                ROUND(price / :group_size, 2) AS price_per_person
            FROM 
                Listings
            WHERE 
                accommodates >= :group_size
                AND price IS NOT NULL
            {order_by_clause}
            LIMIT 10;
        """,
        'params': [
            ('group_size', 'Enter group size (number of people):', int)
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'accommodates', 'price_per_person']
    },
    6: {
        'description': 'Neighbourhoods offering the best value for families with listings under ZAR X per night, considering key amenities.',
        'sql': """
            SELECT 
                neighbourhood,
                ROUND(AVG(price), 2) AS average_price,
                COUNT(*) AS listings_count
            FROM 
                Listings
            WHERE 
                accommodates >= :min_guests
                AND price <= :max_price
                AND array_length(string_to_array(amenities, ','), 1) >= :min_amenities
            GROUP BY 
                neighbourhood
            {order_by_clause}
            LIMIT 10;
        """,
        'params': [
            ('min_guests', 'Enter minimum number of guests (e.g., for families):', int),
            ('max_price', 'Enter maximum price per night (ZAR):', float),
            ('min_amenities', 'Enter minimum number of key amenities:', int)
        ],
        'sortable_columns': ['neighbourhood', 'average_price', 'listings_count']
    },
    7: {
        'description': 'Compare average prices of listings that accommodate X people in top-rated areas versus less popular areas.',
        'sql': """
            WITH top_vs_less AS (
                SELECT 
                    l.neighbourhood,
                    CASE 
                        WHEN nr.avg_rating >= :rating_threshold THEN 'Top-rated'
                        ELSE 'Less popular'
                    END AS area_type,
                    l.price
                FROM 
                    Listings l
                JOIN 
                    neighbourhood_ratings nr ON l.neighbourhood = nr.neighbourhood
                WHERE 
                    l.accommodates >= :group_size
                    AND l.price IS NOT NULL
            )
            SELECT 
                area_type,
                ROUND(AVG(price), 2) AS average_price
            FROM 
                top_vs_less
            GROUP BY 
                area_type
            {order_by_clause};
        """,
        'params': [
            ('rating_threshold', 'Enter rating threshold to define top-rated areas (e.g., 4):', float),
            ('group_size', 'Enter group size (number of people):', int)
        ],
        'sortable_columns': ['area_type', 'average_price']
    },
    8: {
        'description': 'Listings for groups of X people with the most consistent positive guest feedback and priced below the citywide average of ZAR X per night.',
        'sql': """
            SELECT 
                l.name,
                l.neighbourhood,
                'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price,
                l.review_scores_rating,
                l.accommodates
            FROM 
                Listings l,
                average_price ap
            WHERE 
                l.accommodates >= :group_size
                AND l.price < ap.avg_price
                AND l.review_scores_rating > :rating_threshold
            {order_by_clause}
            LIMIT 10;
        """,
        'params': [
            ('group_size', 'Enter group size (number of people):', int),
            ('rating_threshold', 'Enter minimum review score rating:', float)
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'accommodates']
    },
    9: {
        'description': 'Relationship between listing price and the number of amenities in the most popular neighbourhoods for large groups, focusing on listings under ZAR X per night.',
        'sql': """
            SELECT 
                l.name,
                l.neighbourhood,
                'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price,
                array_length(string_to_array(l.amenities, ','), 1) AS amenities_count
            FROM 
                Listings l
            JOIN 
                top_neighbourhoods tn ON l.neighbourhood = tn.neighbourhood
            WHERE 
                l.accommodates >= :group_size
                AND l.price < :max_price
            {order_by_clause};
        """,
        'params': [
            ('group_size', 'Enter group size (number of people):', int),
            ('max_price', 'Enter maximum price per night (ZAR):', float)
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'amenities_count']
    },
    10: {
        'description': 'Best value listings in under-served areas based on guest ratings and price for groups of X people, particularly those under ZAR X per night.',
        'sql': """
            SELECT 
                l.name,
                l.neighbourhood,
                'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price,
                l.review_scores_rating,
                l.accommodates
            FROM 
                Listings l
            JOIN 
                under_served_neighbourhoods usn ON l.neighbourhood = usn.neighbourhood
            WHERE 
                l.accommodates >= :group_size
                AND l.price < :max_price
                AND l.review_scores_rating > :rating_threshold
            {order_by_clause}
            LIMIT 10;
        """,
        'params': [
            ('group_size', 'Enter group size (number of people):', int),
            ('max_price', 'Enter maximum price per night (ZAR):', float),
            ('rating_threshold', 'Enter minimum review score rating:', float)
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'accommodates']
    },
    11: {  # Combined Query for Sorting Listings
        'description': 'Airbnb listings sorted by specified criteria with filtering and sorting options.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                accommodates
            FROM 
                Listings
            WHERE 
                price IS NOT NULL
            {order_by_clause}
            LIMIT 100 OFFSET 0;
        """,
        'params': [],  # No parameters needed since filtering by Cape Town is removed
        'sortable_columns': ['name', 'neighbourhood', 'price', 'accommodates']
    },
    # Removed Query 12 as it's now combined with Query 11
    13: {
        'description': 'Airbnb listings rated X stars or higher.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                review_scores_rating,
                accommodates
            FROM 
                Listings
            WHERE 
                review_scores_rating >= :min_rating
            {order_by_clause}
            LIMIT 100 OFFSET 0;
        """,
        'params': [
            ('min_rating', 'Enter minimum review score rating:', float)
            # Removed 'offset' as it's no longer needed
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'accommodates']
    },
    14: {
        'description': 'Listings ranked by a weighted score (Price 60%, Rating 40%) to balance affordability and guest satisfaction.',
        'sql': """
            WITH price_stats AS (
                SELECT 
                    MIN(price) AS min_price,
                    MAX(price) AS max_price
                FROM 
                    Listings
                WHERE 
                    price IS NOT NULL
            ), rating_stats AS (
                SELECT 
                    MIN(review_scores_rating) AS min_rating,
                    MAX(review_scores_rating) AS max_rating
                FROM 
                    Listings
                WHERE 
                    review_scores_rating IS NOT NULL
            )
            SELECT 
                l.name,
                l.neighbourhood,
                'R' || TO_CHAR(l.price, 'FM999,999,999.00') AS price,
                l.review_scores_rating,
                ROUND(
                    0.6 * (1 - ((l.price - ps.min_price) / NULLIF(ps.max_price - ps.min_price, 0))) +
                    0.4 * ((l.review_scores_rating - rs.min_rating) / NULLIF(rs.max_rating - rs.min_rating, 0)),
                    2
                ) AS weighted_score
            FROM 
                Listings l
            CROSS JOIN 
                price_stats ps
            CROSS JOIN 
                rating_stats rs
            WHERE 
                l.price IS NOT NULL
                AND l.review_scores_rating IS NOT NULL
            {order_by_clause}
            LIMIT 100 OFFSET 0;
        """,
        'params': [
            # Removed 'limit' and 'offset'
        ],
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'weighted_score']
    },
    15: {
        'description': 'Likelihood of a listing being available based on availability percentage.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                availability_365,
                ROUND((availability_365 / 365.0) * 100, 2) AS availability_percentage
            FROM 
                Listings
            {order_by_clause}
            LIMIT 100 OFFSET 0;
        """,
        'params': [],  # Removed 'limit' and 'offset'
        'sortable_columns': ['name', 'neighbourhood', 'price', 'availability_365', 'availability_percentage']
    },
    16: {
        'description': 'Airbnb listings with the highest overall guest ratings.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                review_scores_rating,
                accommodates
            FROM 
                Listings
            WHERE 
                review_scores_rating IS NOT NULL
            {order_by_clause}
            LIMIT 100 OFFSET 0;
        """,
        'params': [],  # Removed 'limit' and 'offset'
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'accommodates']
    },
    17: {
        'description': 'Airbnb listings with the lowest overall guest ratings.',
        'sql': """
            SELECT 
                name,
                neighbourhood,
                'R' || TO_CHAR(price, 'FM999,999,999.00') AS price,
                review_scores_rating,
                accommodates
            FROM 
                Listings
            WHERE 
                review_scores_rating IS NOT NULL
            {order_by_clause}
            LIMIT 100 OFFSET 0;
        """,
        'params': [],  # Removed 'limit' and 'offset'
        'sortable_columns': ['name', 'neighbourhood', 'price', 'review_scores_rating', 'accommodates']
    },
    18: {
        'description': 'Listings categorized by room type with counts.',
        'sql': """
            SELECT 
                room_type,
                COUNT(*) AS listings_count
            FROM 
                Listings
            GROUP BY 
                room_type
            {order_by_clause};
        """,
        'params': [],
        'sortable_columns': ['room_type', 'listings_count']
    },
    # Add additional queries here as needed...
}
