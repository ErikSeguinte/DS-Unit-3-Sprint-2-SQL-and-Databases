import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2
from psycopg2.extras import execute_values

dbname = os.getenv("user")
user = os.getenv("user")
host = os.getenv("server")
password = os.getenv("password")

# ### Connect to ElephantSQL-hosted PostgreSQL
conn = psycopg2.connect(dbname=dbname, user=user,
                         password=password, host=host)
# ### A "cursor", a structure to iterate over db records to perform queries
cur = conn.cursor()
# ### An example query
# cur.execute('SELECT * from test_table;')
# ### Note - nothing happened yet! We need to actually *fetch* from the cursor
# print(cur.fetchone())

path = Path('titanic.csv')
df = pd.read_csv(path)


ls = list(df.itertuples(index=False, name=None))

q = """insert into titanic(survived, passenger_class, name, sex, age, siblings_spouses, parents_children, fare) values %s"""

execute_values(cur, q, ls) 
conn.commit()
cur.close()
conn.close()