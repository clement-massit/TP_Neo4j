import pandas
import api
import random

df = pandas.read_csv("communes-departement-region.csv")
#print(df[:100])

api.delete_all_query()



data = [df["code_departement"], df["nom_departement"]]
headers = ["code", "nom"]
departement = pandas.concat(data, axis=1, keys=headers).drop_duplicates()
departement = departement[departement['nom'].notna()]

query = ""

for index, row in departement.iterrows():
    query += api.create_node(row['nom'].replace("'", "").replace("-", "").replace(" ", ""),"departement", [["nom", row['nom'].replace("'", " ")],["code", row['code']]]) + " "

api.exectute_request(query)

df = df.drop_duplicates(subset='code_commune_INSEE')

query = ""
for index, row in df[:1000].iterrows():
    query += api.create_node(row['nom_commune_postal'].replace(" ", ""),"commune", [["nom", row['nom_commune'].replace("'", " ")]])
api.exectute_request(query)

for index, row in df[:1000].sample(n=200).iterrows():
    end = api.get_id(api.get_node("code", row["code_departement"]))
    start = api.get_id(api.get_node("nom",row['nom_commune'].replace("'", " ")))
    api.exectute_request(api.create_edge(start, end, "Est_dans_le_département",[]))