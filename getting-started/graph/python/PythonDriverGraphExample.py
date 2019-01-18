from dse.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
from dse.graph import GraphOptions
from dse_graph import DseGraph

def execute_if_not_exists(session, exists_stmt, create_stmt, execution_profile=None):
    exists_result_set = session.execute_graph(exists_stmt, execution_profile = execution_profile)
    for row in exists_result_set:
        exists = row
    if (str(exists) != "True"):
        session.execute_graph(create_stmt, execution_profile = execution_profile)

def create_schema(session, graph_name):

    exists_graph_stmt = "system.graph(\"%s\").exists()" % graph_name
    create_graph_stmt = "system.graph(\"%s\").create()" % graph_name

    exists_capital_property_key_stmt = "schema.propertyKey(\"capital\").exists()"
    create_capital_property_key_stmt = "schema.propertyKey(\"capital\").Text().single().create()"

    exists_name_property_key_stmt = "schema.propertyKey(\"name\").exists()"
    create_name_property_key_stmt = "schema.propertyKey(\"name\").Text().single().create()"

    exists_continent_vertex_label_stmt = "schema.vertexLabel(\"continent\").exists()"
    create_continent_vertex_label_stmt = "schema.vertexLabel(\"continent\").partitionKey(\"name\").create()"

    exists_country_certex_label_stmt = "schema.vertexLabel(\"country\").exists()"
    create_country_certex_label_stmt = "schema.vertexLabel(\"country\").partitionKey(\"name\").properties(\"capital\").create()"
       
    exists_has_country_edge_label_stmt = "schema.edgeLabel(\"has_country\").exists()"
    create_has_country_edge_label_stmt = "schema.edgeLabel(\"has_country\").connection(\"continent\", \"country\").create()"

    execute_if_not_exists(session, exists_graph_stmt, create_graph_stmt, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
    execute_if_not_exists(session, exists_capital_property_key_stmt, create_capital_property_key_stmt, 'schema')
    execute_if_not_exists(session, exists_name_property_key_stmt, create_name_property_key_stmt, 'schema')
    execute_if_not_exists(session, exists_continent_vertex_label_stmt, create_continent_vertex_label_stmt, 'schema')
    execute_if_not_exists(session, exists_country_certex_label_stmt, create_country_certex_label_stmt, 'schema')
    execute_if_not_exists(session, exists_has_country_edge_label_stmt, create_has_country_edge_label_stmt, 'schema')

def add_continent_vertex(session, graph_traversal_source, continent_name):
    g = graph_traversal_source
    session.execute_graph(DseGraph.query_from_traversal(g.addV("continent").property("name", continent_name)))

def add_country_vertex(session, graph_traversal_source, country_name, capital_name):
    g = graph_traversal_source
    session.execute_graph(DseGraph.query_from_traversal(g.addV("country").property("name", country_name).property("capital", capital_name)))

def add_has_country_edge(session, graph_traversal_source, continent_name, country_name):
    g = graph_traversal_source
    session.execute_graph(DseGraph.query_from_traversal(g.addE("has_country").from_(g.V().has("continent", "name", continent_name)).to(g.V().has("country", "name", country_name))))


graph_name = "world_graph"
ep_schema = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph_name))
ep = DseGraph.create_execution_profile(graph_name)

cluster = Cluster(["127.0.0.1"], execution_profiles={'schema': ep_schema, EXEC_PROFILE_GRAPH_DEFAULT: ep})
session = cluster.connect()
g = DseGraph.traversal_source(session=session)

create_schema(session, graph_name)

add_continent_vertex(session, g, "Europe")
add_continent_vertex(session, g, "Asia")
add_continent_vertex(session, g, "North America")

add_country_vertex(session, g, "United Kingdom", "London")
add_country_vertex(session, g, "Germany", "Berlin")
add_country_vertex(session, g, "Spain", "Madrid")
add_country_vertex(session, g, "China", "Beijing")
add_country_vertex(session, g, "United States", "Washington DC")

add_has_country_edge(session, g, "Europe", "United Kingdom")
add_has_country_edge(session, g, "Europe", "Germany")
add_has_country_edge(session, g, "Europe", "Spain")
add_has_country_edge(session, g, "Asia", "China")
add_has_country_edge(session, g, "North America", "United States")

rs = session.execute_graph(DseGraph.query_from_traversal(g.V().has("continent", "name", "Europe").outE("has_country").inV()))

for vertex in rs:
    print "The capital of European country " + vertex.properties['name'][0].value + " is " + vertex.properties['capital'][0].value

cluster.shutdown()
    