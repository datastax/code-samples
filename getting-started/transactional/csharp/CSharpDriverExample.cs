using Dse;
using System;

namespace csharp
{
    class CSharpDriverExample
    {
        static void Main(string[] args)
        {
            IDseCluster cluster = DseCluster.Builder()
                                .AddContactPoint("10.200.176.121")
                                .Build();
            IDseSession session = cluster.Connect();

            string keyspace = "world_ks";
            string table = "world_tbl";

            string createKeyspaceStmt = $"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}";
            string createTableStmt = $"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (continent text, country text, capital text, PRIMARY KEY (continent, country))";
            
            session.Execute(createKeyspaceStmt);
            session.Execute(createTableStmt);

            var insertStmt = session.Prepare($"INSERT INTO {keyspace}.{table} (continent, country, capital) VALUES (?,?,?)");

            session.Execute(insertStmt.Bind("Europe", "United Kingdom", "London"));
            session.Execute(insertStmt.Bind("Europe", "Germany", "Berlin"));
            session.Execute(insertStmt.Bind("Europe", "Spain", "Madrid"));
            session.Execute(insertStmt.Bind("Asia", "China", "Beijing"));
            session.Execute(insertStmt.Bind("North America", "United States", "Washington DC"));

            string readStmt = $"SELECT country, capital FROM {keyspace}.{table} WHERE continent = 'Europe'";

            var resultSet = session.Execute(readStmt);

            foreach (var row in resultSet){
                Console.WriteLine($"The capital of European country {row["country"]} is {row["capital"]}");
            }

            cluster.Shutdown();
        }
    }
}




