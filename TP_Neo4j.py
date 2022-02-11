# import the neo4j driver for Python
from neo4j import GraphDatabase
import pandas

# Database Credentials

uri             = "bolt://localhost:7687"
userName        = "neo4j"
password        = "password"

# Connect to the neo4j database server
graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

def get_all_nodes():
    all_nodes_query = "MATCH (x:university) RETURN x"
    nodes = graphDB_Session.run(all_nodes_query)
    return nodes

def get_node(attribut, value):
    one_node_query = "MATCH (x {" + attribut +": '" + value + "'}) RETURN x"
    node = graphDB_Session.run(one_node_query)
    return node

def get_all_edges():
    all_edges_query = "match (x) -[r]-> (y) return r"
    all_edges = graphDB_Session.run(all_edges_query)
    return all_edges

def get_edge(attribut, value, is_str):
    if is_str:
        one_edge_query = "match (x) -[r {" + attribut +": '" + value + "'}]-> (y) return r"
    else:
        one_edge_query = "match (x) -[r {" + attribut +": " + value + "}]-> (y) return r"
    edge = graphDB_Session.run(one_edge_query)
    return node

def create_node(name, type, attributs):
    create_query = "CREATE (" + name + ":" + type

    if len(attributs) == 1:
        create_query += "{" +attributs[0][0] + ": '" + attributs[0][1] + "'})"
    elif len(attributs) == 0:
        create_query += ")"
    else:
        create_query += "{" 
        for attribut in attributs:
            create_query += attribut[0] + ": '" + attribut[1] + "',"
        create_query = create_query[:-1]
        create_query += "})"
    graphDB_Session.run(create_query)

def create_edge(start, end, type_connection, attributs):


    create_query = "CREATE (" + start + ")-[:" + type_connection   
    if len(attributs) == 1:
        create_query += "{" + attributs[0][0] + ": '" + attributs[0][1] + "'}]"
    elif len(attributs) == 0:
        create_query += "]"
    else:
        create_query += "{" 
        for attribut in attributs:
            create_query += attribut[0] + ": '" + attribut[1] + "',"
        create_query = create_query[:-1]
        create_query += "}]"
    create_query += "->(" + end + ")" 
     

df = pandas.read_csv("communes-departement-region.csv")
print(df[:100])
