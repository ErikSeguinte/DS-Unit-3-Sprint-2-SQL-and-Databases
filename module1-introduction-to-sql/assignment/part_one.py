import sqlite3
from pathlib import Path

path = Path("module1-introduction-to-sql/rpg_db.sqlite3")

conn = sqlite3.connect(path)
# conn.row_factory = sqlite3.Row
c = conn.cursor()

q = """
select 
	count(DISTINCT character_id)

from charactercreator_character;
"""

result = c.execute(q).fetchone()

print(f"The number of characters: {result[0]}")

classes = ["cleric", "fighter", "mage", "necromancer", "thief"]

for char_class in classes:
    q = f"""select 
	            count(DISTINCT character_ptr_id)

            from charactercreator_{char_class};
        """
    try:
        result = c.execute(q).fetchone()
    except sqlite3.OperationalError:
        q = f"""select 
                    count(DISTINCT mage_ptr_id) as total

                from charactercreator_{char_class};
            """
        result = c.execute(q).fetchone()

    print(f"The number of {char_class} characters: {result[0]}")


# Items
q = """
    select 
        count(distinct item_id)
    from armory_item;
    """

result = c.execute(q).fetchone()
item_count = result[0]
print()
print(f"Total number of items: {item_count}")

q = """
    select 
        count(distinct item_ptr_id)
    from armory_weapon;
    """

result = c.execute(q).fetchone()
weapon_count = result[0]
print(f"Total number of weapons: {weapon_count}")
print(f"Total number of non-weapons: {item_count - weapon_count}")


q = """
    select
        items.character_id as character,
        characters.name,
        count(item_id) as number_of_items
    from
        charactercreator_character_inventory as items,
        charactercreator_character as characters
    where
        characters.character_id = items.character_id
    group by
        character
    limit
        10
    """
results = c.execute(q).fetchall()

print("\nFirst 20 character inventories:")
for i in results:
    print(i)

sq = q
q = f"""
    select
    avg(number_of_items)
    from ({sq});
    """

results = c.execute(q).fetchone()

print(f"Average item count: {results[0]}")


q = """
SELECT
    characters.name,
    count (items.item_id) as number_of_weapons
FROM
    charactercreator_character as characters
    JOIN charactercreator_character_inventory as inv on characters.character_id = inv.character_id
    join armory_item as items on inv.item_id = items.item_id
    join armory_weapon as weapons on items.item_id = weapons.item_ptr_id
group by
    inv.character_id
limit
    20
"""

results = c.execute(q).fetchall()

print("\nFirst 20 weapon inventories:")
for i in results:
    print(i)

sq = q
q = f"""
    select
    avg(number_of_weapons)
    from ({sq});
    """
results = c.execute(q).fetchone()
print(f"Average weapon count: {results[0]}")