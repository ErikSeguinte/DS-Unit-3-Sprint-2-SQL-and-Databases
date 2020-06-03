import pymongo, sqlite3, pymongo, os
import dns
import pandas as pd
import numpy as np
import contextlib
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()


path = Path("module1-introduction-to-sql/rpg_db.sqlite3")

with contextlib.closing(sqlite3.connect(path)) as con:

    with con as c:

        q = """
        select 
            count(DISTINCT character_id)

        from charactercreator_character;
        """

        result = c.execute(q).fetchone()
        #print(result)

        char_tables_list= [
            'character',
            'character_inventory',
            'mage',
            'necromancer',
            'thief',
            'cleric',
            'fighter',
        ]

        tables = dict()

        for s in char_tables_list:
            tables[s] = pd.read_sql(f"select * from charactercreator_{s}", con)

            #print(tables[s].head())

        armory_tables_list = [
            'item',
            'weapon'
        ]

        armory = dict()
        for s in armory_tables_list:
            armory[s] = pd.read_sql(f"select * from armory_{s}", con)
            #print(armory[s].head())


uri = os.getenv("mongo_db_uri")

client = pymongo.MongoClient(uri)

db = client['rpg']

for s in char_tables_list:
    col = db[s]
    col.drop()
    
    df = tables[s]
    col.insert_many(df.to_dict(orient='records'))

for s in armory_tables_list:
    col = db[s]
    col.drop()
    
    df = armory[s]
    col.insert_many(df.to_dict(orient='records'))

