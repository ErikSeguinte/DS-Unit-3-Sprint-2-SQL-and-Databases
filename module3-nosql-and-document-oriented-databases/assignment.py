from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
from urllib import parse
import os
import pymongo
import dns

uri = os.getenv("mongo_db_uri")
#uri = parse.quote_plus(uri)

client = pymongo.MongoClient(uri)
db = client.test

result = db.test.insert_one({'stringy key': [2, 'thing', 3]})
print(result.inserted_id)
print(db.test.find_one({'stringy key': [2, 'thing', 3]}))

pokemon = {}

pokemon['pikachu'] = {
    'hp': 211,
    'attack': 160,
    'defense': 116,
    'special attack': 122,
    'special defense': 136,
    'speed': 216
}

pokemon['pichu'] = {
    'hp': 181,
    'attack': 127,
    'defense': 66,
    'special attack': 95,
    'special defense': 106,
    'speed': 156
}

pokemon['charmander'] = {
    'hp': 219,
    'attack': 154,
    'defense': 122,
    'special attack': 140,
    'special defense': 136,
    'speed': 166
}

pokemon['bulbasaur'] = {
    'hp': 231,
    'attack': 147,
    'defense': 134,
    'special attack': 149,
    'special defense': 166,
    'speed': 126
}

pokemon['squirtle'] = {
    'hp': 229,
    'attack': 145,
    'defense': 166,
    'special attack': 122,
    'special defense': 164,
    'speed': 122
}

pokemon['mewtwo'] = {
    'hp': 353,
    'attack': 281,
    'defense': 216,
    'special attack': 309,
    'special defense': 216,
    'speed': 296
}


pokemon_list = []
for key in pokemon:
    mon = pokemon[key]
    mon['name'] = key
    pokemon_list.append(mon)


db.test.insert_many(pokemon_list)

for x in db.test.find():
    print(x)