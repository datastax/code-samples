using Dse;
using Dse.Geometry;
using System;

namespace csharp
{
    class CSharpDriverSearchExample
    {
        static void Main(string[] args)
        {
            IDseCluster cluster = DseCluster.Builder()
                                .AddContactPoint("10.200.176.121")
                                .Build();
            IDseSession session = cluster.Connect();

            string keyspace = "world_ks";
            string table = "world_search_tbl";

            string createKeyspaceStmt = $"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};";
            string createTableStmt = $"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (continent text, country text, capital text, location 'PointType', PRIMARY KEY (continent, country));";
            string createSearchIndexStmt = $"CREATE SEARCH INDEX IF NOT EXISTS ON {keyspace}.{table} WITH COLUMNS location;";

            session.Execute(createKeyspaceStmt);
            session.Execute(createTableStmt);
            session.Execute(createSearchIndexStmt);

            var insertStmt = session.Prepare($"INSERT INTO {keyspace}.{table} (continent, country, capital, location) VALUES (?,?,?,?)");

            session.Execute(insertStmt.Bind("Europe", "United Kingdom", "London", new Point(51.5074, 0.1278)));
            session.Execute(insertStmt.Bind("Europe", "Germany", "Berlin", new Point(52.5200, 13.4050)));
            session.Execute(insertStmt.Bind("Europe", "Spain", "Madrid", new Point(40.4168, 3.7038)));
            session.Execute(insertStmt.Bind("Asia", "China", "Beijing", new Point(39.9042, 116.4074)));
            session.Execute(insertStmt.Bind("North America", "United States", "Washington DC", new Point(38.9072, 77.0369)));

            string solrQuery = "{ \"q\":\"*:*\", \"fq\":\"location:\\\"IsWithin(BUFFER(POINT(45 0), 20.0))\\\"\" }";
            string readStmt = $"SELECT country, capital FROM {keyspace}.{table} WHERE solr_query = '{solrQuery}'";

            var resultSet = session.Execute(readStmt);

            foreach (var row in resultSet){
                Console.WriteLine($"The capital of European country {row["country"]} is {row["capital"]}");
            }

            cluster.Shutdown();
        }
    }
}




