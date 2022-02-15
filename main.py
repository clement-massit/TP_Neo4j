import pandas
import api
import random

df = pandas.read_csv("communes-departement-region.csv")
#print(df[:100])

# Clear the database
api.delete_all_query()

# Get the data to make the departments nodes
data = [df["code_departement"], df["nom_departement"]]
headers = ["code", "nom"]
departement = pandas.concat(data, axis=1, keys=headers).drop_duplicates()
departement = departement[departement['nom'].notna()]

# Create the query to create all the departements nodes
query = ""
for index, row in departement.iterrows():
    query += api.create_node(row['nom'].replace("'", "").replace("-", "").replace(" ", ""),"departement", [["nom", row['nom'].replace("'", " ")],["code", row['code']]]) + " "

api.exectute_request(query)

df = df.drop_duplicates(subset='code_commune_INSEE')

query = ""

for index, row in df[:1000].iterrows():
    query += api.create_node(row['nom_commune_postal'].replace(" ", ""),"commune", [["nom", row['nom_commune'].replace("'", " ")]])
api.exectute_request(query)

number = 0
query_match = ""
query_create = ""
for index, row in df[:1000].iterrows():

    city_name = row['nom_commune'].replace("'", " ")
    departement_code = row["code_departement"]

    res = api.link_city_departement(city_name, departement_code, number)
    query_match += res[0] + " "
    query_create += res[1] + " "
    number += 1
    print(number)

query = query_match + query_create
print(query)
api.exectute_request(query)

"""end = api.get_id(api.get_node("code", row["code_departement"]))
start = api.get_id(api.get_node("nom",row['nom_commune'].replace("'", " ")))
api.exectute_request(api.create_edge(start, end, "Est_dans_le_département",[]))"""