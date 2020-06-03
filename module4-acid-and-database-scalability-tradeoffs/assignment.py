import pymongo, sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

path = Path("module1-introduction-to-sql/rpg_db.sqlite3")

with sqlite3.connect(path) as con:

    c = con.cursor()

    q = """
    select 
        count(DISTINCT character_id)

    from charactercreator_character;
    """

    result = c.execute(q).fetchone()
    print(result)

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

        print(tables[s].head())

    armory_tables_list = [
        'item',
        'weapon'
    ]

    armory = dict()
    for s in armory_tables_list:
        armory[s] = pd.read_sql(f"select * from armory_{s}", con)
        print(armory[s].head())


