from dse.cluster import Cluster
from dse.util import Point

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

keyspace = "world_ks"
table = "world_search_tbl"

create_keyspace_stmt = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};" % keyspace
create_table_stmt = "CREATE TABLE IF NOT EXISTS %s.%s (continent text, country text, capital text, location 'PointType', PRIMARY KEY (continent, country));" % (keyspace, table)
create_search_index_stmt = "CREATE SEARCH INDEX IF NOT EXISTS ON %s.%s WITH COLUMNS location;" % (keyspace, table)

session.execute(create_keyspace_stmt)
session.execute(create_table_stmt)
session.execute(create_search_index_stmt)

insert_stmt = session.prepare("INSERT INTO %s.%s (continent, country, capital, location) VALUES (?,?,?,?);" % (keyspace, table))

session.execute(insert_stmt, ["Europe", "United Kingdom", "London", Point(51.5074, 0.1278)])
session.execute(insert_stmt, ["Europe", "Germany", "Berlin", Point(52.5200, 13.4050)])
session.execute(insert_stmt, ["Europe", "Spain", "Madrid", Point(40.4168, 3.7038)])
session.execute(insert_stmt, ["Asia", "China", "Beijing", Point(39.9042, 116.4074)])
session.execute(insert_stmt, ["North America", "United States", "Washington DC", Point(38.9072, 77.0369)])

solr_query = "{ \"q\":\"*:*\", \"fq\":\"location:\\\"IsWithin(BUFFER(POINT(45 0), 20.0))\\\"\" }"
read_stmt = "SELECT country, capital FROM %s.%s WHERE solr_query = '%s'" % (keyspace, table, solr_query)

result_set = session.execute(read_stmt)

for row in result_set:
    print "The capital of European country " + row.country + " is " + row.capital

cluster.shutdown()