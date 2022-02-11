import api

api.delete_all_query()
api.exectute_request(api.create_node("a", "noeud", [["name", "abc"]]))
api.exectute_request(api.create_node("b", "noeud", [["name", "def"]]))

api.exectute_request(api.create_edge(
    api.get_id(api.get_node("name", "abc")),
    api.get_id(api.get_node("name", "def")), "connects", [["name", "liaison"]]))