import pymongo, sqlite3, pymongo, os, psycopg2
import dns
import pandas as pd
import numpy as np
import contextlib
from pathlib import Path
from pprint import pprint

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

def count_inventory(db):
    inventories = db['character_inventory']



    pipeline = [
        {"$group": { "_id": "$character_id", "count": {"$sum":1}}},
        {"$limit": 20}
    ]

    for i in inventories.aggregate(pipeline):
        pprint(i)

def count_weapons(db):
    inventories = db['character_inventory']
    weapons = db['weapon']
    items = db['item']

    pipeline = [
        {"$lookup":{
            "from":'weapon',
            "localField":'item_id',
            'foreignField':'item_ptr_id',
            'as':'item_stats'
        }},
        {'$match':{
            'item_stats': {"$ne":[]}
        }},
        {"$group": { "_id": "$character_id", "count": {"$sum":1}}},
        {"$limit": 20}
    ]

    print()
    for i in inventories.aggregate(pipeline):
        pprint(i)
        pass

def average_items(db):
    inventories = db['character_inventory']

    pipeline = [
        {"$group": { "_id": "$character_id", "count": {"$sum":1}}},
        {"$group": { "_id": None, "average": {"$avg":'$count'}}},
    ]

    for i in inventories.aggregate(pipeline):
        pprint(i)
        pass



def average_weapons(db):
    inventories = db['character_inventory']
    weapons = db['weapon']
    items = db['item']

    pipeline = [
        {"$lookup":{
            "from":'weapon',
            "localField":'item_id',
            'foreignField':'item_ptr_id',
            'as':'item_stats'
        }},
        {'$match':{
            'item_stats': {"$ne":[]}
        }},
        {"$group": { "_id": "$character_id", "count": {"$sum":1}}},
        {"$group": { "_id": None, "average": {"$avg":'$count'}}},
    ]

    for i in inventories.aggregate(pipeline):
        pprint(i)
        pass
    

    def count_survivors(c:sqlite3.Cursor):


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
    # Number of items: 174
    # Number of weapons: 37
    # Number of non-weapons: 137

    print()
    count_inventory(db)
    # {'_id': 236, 'count': 1}
    # {'_id': 259, 'count': 5}
    # {'_id': 32, 'count': 2}
    # {'_id': 4, 'count': 4}
    # {'_id': 171, 'count': 4}
    # {'_id': 172, 'count': 2}
    # {'_id': 256, 'count': 2}
    # {'_id': 122, 'count': 2}
    # {'_id': 198, 'count': 5}
    # {'_id': 164, 'count': 1}
    # {'_id': 219, 'count': 1}
    # {'_id': 155, 'count': 4}
    # {'_id': 187, 'count': 1}
    # {'_id': 160, 'count': 5}
    # {'_id': 69, 'count': 3}
    # {'_id': 13, 'count': 4}
    # {'_id': 119, 'count': 5}
    # {'_id': 208, 'count': 2}
    # {'_id': 186, 'count': 2}
    # {'_id': 232, 'count': 4}

    print()

    average_items(db)
    # {'_id': None, 'average': 2.9735099337748343}


    count_weapons(db)
    # {'_id': 55, 'count': 2}
    # {'_id': 212, 'count': 1}
    # {'_id': 168, 'count': 1}
    # {'_id': 5, 'count': 2}
    # {'_id': 251, 'count': 1}
    # {'_id': 30, 'count': 1}
    # {'_id': 112, 'count': 1}
    # {'_id': 145, 'count': 1}
    # {'_id': 7, 'count': 1}
    # {'_id': 88, 'count': 1}
    # {'_id': 247, 'count': 2}
    # {'_id': 263, 'count': 1}
    # {'_id': 51, 'count': 1}
    # {'_id': 169, 'count': 1}
    # {'_id': 271, 'count': 1}
    # {'_id': 216, 'count': 1}
    # {'_id': 152, 'count': 1}
    # {'_id': 34, 'count': 1}
    # {'_id': 92, 'count': 1}
    # {'_id': 113, 'count': 2}

    average_weapons(db)
    # {'_id': None, 'average': 1.3096774193548386}



