import sqlite3
import pandas as pd
from pathlib import Path

path = Path("../buddymove_holidayiq.csv")

df = pd.read_csv(path)


write_path = Path("../buddymove_holidayiq.sqlite3")

conn = sqlite3.connect(write_path)
# conn.row_factory = sqlite3.Row
c = conn.cursor()

# df.to_sql('review', conn)

q = "select count('index') from review"

print(f"Number of rows: {c.execute(q).fetchone()[0]}")

q = """
select
    count('user id')
from
    review
where
    nature >= 100
    AND shopping >= 100
"""

print(f"Number of users where Shopping and Nature > 100:{c.execute(q).fetchone()[0]}")

########
#STRETCH
########


# Get Column Names to iterate over
q = """
PRAGMA table_info(review);
"""
results = c.execute(q).fetchall()

col_names = [x[1] for x in results]
print(col_names[2:])

for col in col_names[2:]:
    q = f"""
    select
        avg({col})
    from review
    """
    result = c.execute(q).fetchone()

    print(f"Average {col} : {result[0]}")

