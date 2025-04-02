import mysql.connector
import pandas as pd
from datetime import datetime

def parse_date(date_string):
    """
    Parse a date string into the format YYYY-MM-DD.
    Handles multiple date formats and removes extraneous text.
    """
    cleaned_date = date_string.split('(')[0].strip()

    formats = [
        "%B %d, %Y",  # Full date: October 14, 1994
        "%B %Y",      # Month and year: October 1994
        "%Y"          # Year only: 1994
    ]

    for fmt in formats:
        try:
            parsed_date = datetime.strptime(cleaned_date, fmt)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None

def convert_to_runtime_float(runtime_string):
    """
    Convert a runtime string (e.g., "2 hours 30 minutes") into a float representing hours.
    """
    components = runtime_string.split()
    hours = 0
    minutes = 0

    for i in range(0, len(components), 2):
        value = int(components[i])
        unit = components[i + 1].lower()
        if "hour" in unit:
            hours = value
        elif "minute" in unit:
            minutes = value

    return hours + (minutes / 60)

def safe_convert_to_int(value, default=None):
    """
    Safely convert a value to an integer. If conversion fails, return the default value.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def insert_data(con, cursor):
    """
    Insert data from the IMDB dataset into the MySQL database.
    Handles multiple tables and relationships.
    """
    file_path = "IMDB_Movies_Dataset.csv"
    df = pd.read_csv(file_path)

    columns_to_read = [
        'Title', 'Average Rating', 'Director', 'Writer', 'Metascore', 'Cast',
        'Release Date', 'Country of Origin', 'Languages', 'Budget', 'Worldwide Gross', 'Runtime'
    ]

    for col in columns_to_read:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df = df.fillna("")  # Replace NaN with empty strings for consistency

    for i, row in df.iterrows():
        if i % 100 == 0:
            print(f"Processing row {i}...")
        try:
            movie_id = i + 1  # Use the index as the primary key
            movie_title = row['Title']
            avg_rating = float(row['Average Rating']) if row['Average Rating'] else None
            directors = row['Director'].split(", ")
            writers = row['Writer'].split(", ")
            metascore = safe_convert_to_int(row['Metascore'])
            cast = row['Cast'].split(", ")
            formatted_date = parse_date(row['Release Date'])
            countries = row['Country of Origin'].split(", ")
            languages = row['Languages'].split(", ")
            budget = safe_convert_to_int(row['Budget'].replace("$", "").replace(",", "").replace("(estimated)", "").strip())
            revenue = safe_convert_to_int(row['Worldwide Gross'].replace("$", "").replace(",", ""))
            runtime = convert_to_runtime_float(row['Runtime']) if row['Runtime'] else None

            # Insert into Movie table
            cursor.execute(
                """
                INSERT INTO Movie (movie_id, title, average_rating, release_date, budget, revenue, runtime, meta_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (movie_id, movie_title, avg_rating, formatted_date, budget, revenue, runtime, metascore)
            )

            # Insert into Person table (Directors, Writers, Cast)
            persons = set(directors + writers + cast)
            for person in persons:
                cursor.execute(
                    """
                    INSERT IGNORE INTO Person (name)
                    VALUES (%s)
                    """,
                    (person,)
                )

            # Insert relationships into Staff_Movie table
            for actor in cast:
                cursor.execute(
                    """
                    INSERT INTO Staff_Movie (person_name, movie_id, role)
                    VALUES (%s, %s, %s)
                    """,
                    (actor, movie_id, 'actor')
                )

            for director in directors:
                cursor.execute(
                    """
                    INSERT INTO Staff_Movie (person_name, movie_id, role)
                    VALUES (%s, %s, %s)
                    """,
                    (director, movie_id, 'director')
                )

            for writer in writers:
                cursor.execute(
                    """
                    INSERT INTO Staff_Movie (person_name, movie_id, role)
                    VALUES (%s, %s, %s)
                    """,
                    (writer, movie_id, 'writer')
                )

            # Insert into Country and Movie_Country tables
            for country in countries:
                cursor.execute(
                    """
                    INSERT IGNORE INTO Country (country_name)
                    VALUES (%s)
                    """,
                    (country,)
                )
                cursor.execute(
                    """
                    INSERT INTO Movie_Country (movie_id, country_name)
                    VALUES (%s, %s)
                    """,
                    (movie_id, country)
                )

            # Insert into Language and Movie_Language tables
            for language in languages:
                cursor.execute(
                    """
                    INSERT IGNORE INTO Language (language_name)
                    VALUES (%s)
                    """,
                    (language,)
                )
                cursor.execute(
                    """
                    INSERT INTO Movie_Language (movie_id, language_name)
                    VALUES (%s, %s)
                    """,
                    (movie_id, language)
                )

        except mysql.connector.Error as err:
            print(f"Error processing row {i}: {err}")

    con.commit()
    print("Data insertion complete.")
