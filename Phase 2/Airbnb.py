import pandas as pd # type: ignore
from sqlalchemy import create_engine # type: ignore
import os

# Get the directory of the current script
script_dir = os.path.dirname(__file__)

# Read the CSV file into a DataFrame
csv_path = os.path.join(script_dir, '..\\listings.csv')
df = pd.read_csv(csv_path)

# Create a connection to the PostgreSQL database
engine = create_engine('postgresql+psycopg2://postgres:mielies@localhost/airbnb_cape_town')

# Load data into the database
df.to_sql('listings', engine, if_exists='replace', index=False)