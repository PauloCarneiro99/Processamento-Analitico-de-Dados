import os

from tqdm import tqdm
from loguru import logger
from psycopg2.extras import DictCursor
from psycopg2 import connect

if not os.getenv("DB_HOST") or not os.getenv("DB_PASS"):
    raise Exception("No database credentials provided")

tables = ["sexo.sql", "tempo.sql", "local.sql", "indicadores_sociais.sql"]
con = connect(
    host=os.getenv("DB_HOST"),
    dbname="postgres",
    user="postgres",
    password=os.getenv("DB_PASS"),
    cursor_factory=DictCursor,
)
cursor = con.cursor()

logger.info("Start creating table")
for table in tqdm(tables):
    with open(f"src/sqls/{table}", "r") as f:
        sql = " ".join(f.readlines())
    cursor.execute(sql)
logger.info("Done")

con.commit()
con.close()

