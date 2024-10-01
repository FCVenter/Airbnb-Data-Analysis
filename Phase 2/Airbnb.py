import pandas as pd # type: ignore
from sqlalchemy import create_engine # type: ignore

# Read the CSV file into a DataFrame
df = pd.read_csv('listings.csv')

# Create a connection to the PostgreSQL database
engine = create_engine('postgresql+psycopg2://postgres:mielies@localhost/airbnb_cape_town')

# Load data into the database
df.to_sql('listings', engine, if_exists='replace', index=False)
