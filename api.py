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
    all_nodes_query = "MATCH (x) RETURN x"
    nodes = graphDB_Driver.session().run(all_nodes_query)
    print(all_nodes_query)
    return nodes

def get_node(attribut, value):
    one_node_query = "MATCH (x {" + attribut +": '" + value + "'}) RETURN x"
    nodes = graphDB_Driver.session().run(one_node_query)
    print(one_node_query)
    for node in nodes:
        return node

def get_all_edges():
    all_edges_query = "match (x) -[r]-> (y) return r"
    all_edges = graphDB_Driver.session().run(all_edges_query)
    print(all_edges_query)
    return all_edges

def get_edge(attribut, value, is_str):
    if is_str:
        one_edge_query = "match (x) -[r {" + attribut +": '" + value + "'}]-> (y) return r"
    else:
        one_edge_query = "match (x) -[r {" + attribut +": " + value + "}]-> (y) return r"
    edge = graphDB_Driver.session().run(one_edge_query)
    print(one_edge_query)
    return edge

def create_node(name, type, attributs):
    create_query = "CREATE (" + name + ":" + type

    if len(attributs) == 1:
        create_query += " {" +attributs[0][0] + ": '" + attributs[0][1] + "'})"
    elif len(attributs) == 0:
        create_query += ")"
    else:
        create_query += " {" 
        for attribut in attributs:
            create_query += attribut[0] + ": '" + attribut[1] + "',"
        create_query = create_query[:-1]
        create_query += "})"
    print(create_query)
    return create_query

def create_edge(start, end, type_connection, attributs):
    create_query = "MATCH (a) WHERE ID(a) = " + str(start) + " MATCH (b) WHERE ID(b) = " + str(end) + " CREATE (a)-[:" + type_connection   
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
    create_query += "->(b)"
    print(create_query)
    return create_query

def delete_all_query():
    query = "MATCH (x) -[r]-> (y) DELETE r"
    graphDB_Driver.session().run(query)
    query = "MATCH (a) DELETE a"
    graphDB_Driver.session().run(query)

def get_id(node):
    return node['x'].id

def exectute_request(query):
    graphDB_Driver.session().run(query)

def link_city_departement(city_name, departement_code, number):
    query_match = "MATCH (x" + str(number) +  " {code: '" + departement_code + "'}) MATCH (y" +  str(number) + " {nom: '"+city_name+"'})" 
    query_create = "CREATE (x" + str(number) +  ")-[:Est_dans_le_département]->(y" + str(number) +  ")"
    print(query_match,query_create)
    return query_match,query_create
