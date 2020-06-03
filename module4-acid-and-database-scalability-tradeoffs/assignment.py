import pymongo, sqlite3, pymongo, os
import dns
import pandas as pd
import numpy as np
import contextlib
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("mongo_db_uri")

client = pymongo.MongoClient(uri)

db = client["rpg"]

char_tables_list= [
    'character',
    'character_inventory',
    'mage',
    'necromancer',
    'thief',
    'cleric',
    'fighter',
]

armory_tables_list = [
    'item',
    'weapon'
]

def count_classes(db):
    # Number of Characters and classes
    print(f"Number of Characters: {db['character'].count_documents({})}")


    for character_class in char_tables_list[2:]:
        print(f"Number of {character_class}s: {db[character_class].count_documents({})}")


def count_items(db):
    counts = list()
    for s in armory_tables_list:
        count = db[s].count_documents({})
        print(f"Number of {s}s: {count}")
        counts.append(count)

    non_weapons = counts[0] - counts[1]

    print(f"Number of non-weapons: {non_weapons}")

if __name__ == "__main__":
    count_classes(db)
    # OUTPUT
    # Number of Characters: 302
    # Number of mages: 108
    # Number of necromancers: 11
    # Number of thiefs: 51
    # Number of clerics: 75
    # Number of fighters: 68

    count_items(db)