from dse.cluster import Cluster

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

keyspace = "world_ks"
table = "world_tbl"

create_keyspace_stmt = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};" % keyspace
create_table_stmt = "CREATE TABLE IF NOT EXISTS %s.%s (continent text, country text, capital text, PRIMARY KEY (continent, country))" % (keyspace, table)

session.execute(create_keyspace_stmt)
session.execute(create_table_stmt)

insert_stmt = session.prepare("INSERT INTO %s.%s (continent, country, capital) VALUES (?,?,?)" % (keyspace, table))

session.execute(insert_stmt, ["Europe", "United Kingdom", "London"])
session.execute(insert_stmt, ["Europe", "Germany", "Berlin"])
session.execute(insert_stmt, ["Europe", "Spain", "Madrid"])
session.execute(insert_stmt, ["Asia", "China", "Beijing"])
session.execute(insert_stmt, ["North America", "United States", "Washington DC"])

read_stmt = "SELECT country, capital FROM %s.%s WHERE continent = 'Europe'" % (keyspace, table)

result_set = session.execute(read_stmt)

for row in result_set:
    print "The capital of European country " + row.country + " is " + row.capital

cluster.shutdown()