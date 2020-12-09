import os
import math
import pandas as pd

from loguru import logger
from psycopg2.extras import DictCursor, execute_values, execute_batch
from psycopg2 import connect

if not os.getenv("DB_HOST") or not os.getenv("DB_PASS"):
    raise Exception("No database credentials provided")

con = connect(
    host=os.getenv("DB_HOST"),
    dbname="postgres",
    user="postgres",
    password=os.getenv("DB_PASS"),
    cursor_factory=DictCursor,
)

cursor = con.cursor()


# POPULATING LOCAL TABLE

logger.info("populating local table")

insert_sql = """
INSERT INTO SCHEMA_INDI_SOCIAIS.LOCAL (NOME_PAIS, SUB_REGIAO, CONTINENTE)
VALUES (%(nome_pais)s, %(sub_regiao)s, %(continente)s)
"""

df = pd.read_csv("src/dataset/sub_regions_distribution.csv", error_bad_lines=False)[
    ["Region Name", "Sub-region Name", "Country or Area"]
]
df.columns = ["continente", "sub_regiao", "nome_pais"]

execute_batch(cursor, insert_sql, df.to_dict(orient="records"))

# POPULATING SEXO TABLE

logger.info("Populating sexo table")

insert_sql = """
    INSERT INTO SCHEMA_INDI_SOCIAIS.SEXO (SEXO) 
    VALUES ('masculino'), ('feminino')
"""

cursor.execute(insert_sql)


# POPULATING TEMPO TABLE

logger.info("Populating tempo table")

insert_sql = """
INSERT INTO SCHEMA_INDI_SOCIAIS.TEMPO (ANO, SECULO, DECADA) 
VALUES %s
ON CONFLICT (ANO) DO NOTHING
"""

values = []
for year in range(1, 2021):
    century = (year // 100) + 1
    decade = math.floor((year - 1) % 100 / 10) * 10
    values.append((year, century, decade))
execute_values(cursor, insert_sql, values)

# POPULATING INDICADORES SOCIAIS TABLE
cursor.execute("select * from SCHEMA_INDI_SOCIAIS.SEXO")
sexo = {}
for row in cursor.fetchall():
    sexo[row["sexo"]] = row["id_sexo"]

local = {}
cursor.execute("select * from SCHEMA_INDI_SOCIAIS.LOCAL")
for row in cursor.fetchall():
    local[row["nome_pais"].lower()] = row["chave_pais"]


# HDI dataset #

logger.info("fetching hdi data")

d = []
female = pd.read_csv("src/dataset/human_development_index_female.csv")
male = pd.read_csv("src/dataset/human_development_index_male.csv")

male["id_sexo"] = sexo["masculino"]
female["id_sexo"] = sexo["feminino"]

df = pd.concat([male, female])

drop_columns = []
for column in df.columns:
    if "Unnamed" in column:
        drop_columns.append(column)

df.drop(columns=drop_columns, inplace=True)
df.dropna(subset=["Country"], inplace=True)

# fil the year gaps

for _, row in df.iterrows():
    if row["Country"].lower() not in local:
        continue
    for key, value in dict(row).items():
        try:
            d.append(
                dict(
                    chave_pais=local[row["Country"].lower()],
                    ano=int(key),
                    id_sexo=row["id_sexo"],
                    idh=float(value),
                )
            )
        except:
            continue

hdi_df = pd.DataFrame(d)

# LIFE EXPECTANCY #

logger.info("fetching life expectancy data")


female = pd.read_csv("src/dataset/life_expectancy_female.csv")
male = pd.read_csv("src/dataset/life_expectancy_female.csv")


male["id_sexo"] = sexo["masculino"]
female["id_sexo"] = sexo["feminino"]

df = pd.concat([male, female])
df.dropna(subset=["Country Name"], inplace=True)
d = []

for _, row in df.iterrows():
    if row["Country Name"].lower() not in local:
        continue
    for key, value in row.items():
        try:
            d.append(
                dict(
                    chave_pais=local[row["Country Name"].lower()],
                    ano=int(key),
                    id_sexo=row["id_sexo"],
                    expectativa_vida=float(value),
                )
            )
        except:
            continue

life_expectancy = pd.DataFrame(d)

# YEAR OF SCHOOLING #

logger.info("fetching years of schooling data")

male = pd.read_csv("src/dataset/mean_years_of_schooling_male.csv")
female = pd.read_csv("src/dataset/mean_years_of_schooling_female.csv")

male["id_sexo"] = sexo["masculino"]
female["id_sexo"] = sexo["feminino"]

df = pd.concat([male, female])
d = []

drop_columns = []
for column in df.columns:
    if "Unnamed" in column:
        drop_columns.append(column)

df.drop(columns=drop_columns, inplace=True)
df.dropna(subset=["Country"], inplace=True)

for _, row in df.iterrows():
    if row["Country"].lower() not in local:
        continue
    for key, value in dict(row).items():
        try:
            d.append(
                dict(
                    chave_pais=local[row["Country"].lower()],
                    ano=int(key),
                    id_sexo=row["id_sexo"],
                    anos_escolaridade=float(value),
                )
            )
        except:
            continue

years_schooling = pd.DataFrame(d)


final_df = hdi_df.merge(life_expectancy, on=["ano", "chave_pais", "id_sexo"]).merge(
    years_schooling, on=["ano", "chave_pais", "id_sexo"]
)
final_df = final_df.drop(final_df[final_df.ano < 2010].index)


insert_sql = """
INSERT INTO SCHEMA_INDI_SOCIAIS.INDICADORES_SOCIAIS 
(CHAVE_PAIS, ANO, ID_SEXO, IDH, ANOS_ESCOLARIDADE, EXPECTATIVA_VIDA)
VALUES (%(chave_pais)s, %(ano)s, %(id_sexo)s, %(idh)s,%(anos_escolaridade)s, %(expectativa_vida)s)
ON CONFLICT (CHAVE_PAIS, ANO, ID_SEXO) DO NOTHING
"""

execute_batch(cursor, insert_sql, final_df.to_dict(orient="records"))

con.commit()
con.close()
