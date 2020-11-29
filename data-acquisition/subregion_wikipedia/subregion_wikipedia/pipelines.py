import os

from psycopg2 import connect
from psycopg2.extras import DictCursor


class PersistencePipeline:
    def __init__(self):
        if not os.getenv("DB_HOST") or not os.getenv("DB_PASS"):
            raise "No database credentials provided"
        self.con = connect(
            host=os.getenv("DB_HOST"),
            dbname="postgres",
            user="postgres",
            password=os.getenv("DB_PASS"),
            cursor_factory=DictCursor,
        )
        self.cur = self.con.cursor()
        self.insert_sql = """
            INSERT INTO SCHEMA_INDI_SOCIAIS.LOCAL (NOME_PAIS, SUB_REGIAO, CONTINENTE)
            VALUES (%(nome_pais)s,%(nome_regiao)s, %(nome_continente)s)
            ON CONFLICT DO NOTHING
        """

    def process_item(self, item, spider):
        self.cur.execute(self.insert_sql, item)
        self.con.commit()
        return item
