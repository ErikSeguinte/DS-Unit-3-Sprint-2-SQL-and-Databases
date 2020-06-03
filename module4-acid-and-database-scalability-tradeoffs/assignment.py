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

char_tables_list = [
    "character",
    "character_inventory",
    "mage",
    "necromancer",
    "thief",
    "cleric",
    "fighter",
]

armory_tables_list = ["item", "weapon"]


def count_classes(db):
    # Number of Characters and classes
    print(f"Number of Characters: {db['character'].count_documents({})}")

    for character_class in char_tables_list[2:]:
        print(
            f"Number of {character_class}s: {db[character_class].count_documents({})}"
        )


def count_items(db):
    counts = list()
    for s in armory_tables_list:
        count = db[s].count_documents({})
        print(f"Number of {s}s: {count}")
        counts.append(count)

    non_weapons = counts[0] - counts[1]

    print(f"Number of non-weapons: {non_weapons}")


def count_inventory(db):
    inventories = db["character_inventory"]

    pipeline = [
        {"$group": {"_id": "$character_id", "count": {"$sum": 1}}},
        {"$limit": 20},
    ]

    for i in inventories.aggregate(pipeline):
        pprint(i)


def count_weapons(db):
    inventories = db["character_inventory"]
    weapons = db["weapon"]
    items = db["item"]

    pipeline = [
        {
            "$lookup": {
                "from": "weapon",
                "localField": "item_id",
                "foreignField": "item_ptr_id",
                "as": "item_stats",
            }
        },
        {"$match": {"item_stats": {"$ne": []}}},
        {"$group": {"_id": "$character_id", "count": {"$sum": 1}}},
        {"$limit": 20},
    ]

    print()
    for i in inventories.aggregate(pipeline):
        pprint(i)
        pass


def average_items(db):
    inventories = db["character_inventory"]

    pipeline = [
        {"$group": {"_id": "$character_id", "count": {"$sum": 1}}},
        {"$group": {"_id": None, "average": {"$avg": "$count"}}},
    ]

    for i in inventories.aggregate(pipeline):
        pprint(i)
        pass


def average_weapons(db):
    inventories = db["character_inventory"]
    weapons = db["weapon"]
    items = db["item"]

    pipeline = [
        {
            "$lookup": {
                "from": "weapon",
                "localField": "item_id",
                "foreignField": "item_ptr_id",
                "as": "item_stats",
            }
        },
        {"$match": {"item_stats": {"$ne": []}}},
        {"$group": {"_id": "$character_id", "count": {"$sum": 1}}},
        {"$group": {"_id": None, "average": {"$avg": "$count"}}},
    ]

    for i in inventories.aggregate(pipeline):
        pprint(i)
        pass


def count_survivors(c: sqlite3.Cursor):
    pass


def execute_SQL_query(c, s: str):
    c.execute(s)
    results = c.fetchall()
    for r in results:
        pprint(r)


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

    #########
    # Titanic
    #########
    dbname = os.getenv("user")
    user = os.getenv("user")
    host = os.getenv("server")
    password = os.getenv("password")
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

    # Number of Survivors
    c = conn.cursor()

    execute_SQL_query(c, "select count(distinct id) from titanic where survived = 1")
    # 684

    # Number of non-survivors
    execute_SQL_query(c, "select count(distinct id) from titanic where survived = 0")
    # 1090

    # How many passengers survived/died within each class?
    execute_SQL_query(
        c,
        """select passenger_class, count(distinct id) from titanic where survived = 1 group by passenger_class""",
    )
    # OUTPUT
    # (1, 272)
    # (2, 174)
    # (3, 238)
    execute_SQL_query(
        c,
        """select passenger_class, count(distinct id) from titanic where survived = 0 group by passenger_class""",
    )
    # (1, 160)
    # (2, 194)
    # (3, 736)

    # What was the average age of survivors vs nonsurvivors?
    execute_SQL_query(
        c, "select survived, avg(age)  as average from titanic group by survived"
    )
    # OUTPUT
    # (0, Decimal('30.1541284403669725'))
    # (1, Decimal('28.4122807017543860'))

    # What was the average age of each passenger class?
    execute_SQL_query(
        c,
        "select passenger_class, avg(age)  as average from titanic group by passenger_class order by passenger_class",
    )
    # Output
    # (1, Decimal('38.7916666666666667'))
    # (2, Decimal('29.8804347826086957'))
    # (3, Decimal('25.2032854209445585'))

    # What was the average fare by passenger class? By survival?
    execute_SQL_query(
        c,
        "select passenger_class, avg(fare)  as average from titanic group by passenger_class order by passenger_class",
    )
    # OUTPUT
    # (1, 84.154687528257)
    # (2, 20.6621831810993)
    # (3, 13.7077075010452)

    execute_SQL_query(
        c,
        "select survived, avg(fare)  as average from titanic group by survived order by survived",
    )
    # (0, 22.2085840951412)
    # (1, 48.3954076976107)


    # How many siblings/spouses aboard on average, by passenger class? By survival?
    execute_SQL_query(
        c,
        "select avg(siblings_spouses) from titanic"
    )
    # (Decimal('0.52536640360766629087'),)
    execute_SQL_query(
        c,
        "select passenger_class, avg(siblings_spouses) from titanic group by passenger_class order by passenger_class",
    )
    # OUTPUT
    # (1, Decimal('0.41666666666666666667'))
    # (2, Decimal('0.40217391304347826087'))
    # (3, Decimal('0.62012320328542094456'))

    execute_SQL_query(
        c,
        "select survived, avg(siblings_spouses) from titanic group by survived order by survived",
    )
    # (0, Decimal('0.55779816513761467890'))
    # (1, Decimal('0.47368421052631578947'))


    # How many parents/children aboard on average, by passenger class? By survival?
    execute_SQL_query(
        c,
        "select avg(parents_children) from titanic"
    )
    # (Decimal('0.38331454340473506201'),)
    execute_SQL_query(
        c,
        "select passenger_class, avg(parents_children) from titanic group by passenger_class order by passenger_class",
    )
    # OUTPUT
    # (1, Decimal('0.35648148148148148148'))
    # (2, Decimal('0.38043478260869565217'))
    # (3, Decimal('0.39630390143737166324'))

    execute_SQL_query(
        c,
        "select survived, avg(parents_children) from titanic group by survived order by survived",
    )
    # (0, Decimal('0.33211009174311926606'))
    # (1, Decimal('0.46491228070175438596'))



    # Do any passengers have the same name?
    # prints 1 name if 1 or more duplicates exist
    execute_SQL_query(c,
    """select name, count(name) from titanic group by name having count(*) > 1 limit 1"""
    )
    # ('Mrs. (Mantoura Boulos) Moussa', 2)