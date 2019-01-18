using Dse;
using Dse.Graph;
using Gremlin.Net;
using System;
using System.Linq;
using Gremlin.Net.Process.Traversal;

namespace csharp
{
    class CSharpDriverGraphExample
    {
        static void executeIfNotExists(IDseSession session, string existsStmt, string createStmt) {
            var result = session.ExecuteGraph(new SimpleGraphStatement(existsStmt)).First();
            if (result.ToString() != "True"){
                session.ExecuteGraph(new SimpleGraphStatement(createStmt));
            }
        }

        static void createSchema(IDseSession session, string graphName) {

            string existsGraphStmt = $"system.graph(\"{graphName}\").exists()";
            string createGraphStmt = $"system.graph(\"{graphName}\").create()";

            string existsCapitalPropertyKeyStmt = "schema.propertyKey(\"capital\").exists()";
            string createCapitalPropertyStmt = "schema.propertyKey(\"capital\").Text().single().create()";

            string existsNamePropertyKeyStmt = "schema.propertyKey(\"name\").exists()";
            string createNamePropertyKeyStmt = "schema.propertyKey(\"name\").Text().single().create()";

            string existsContinentVertexLabelStmt = "schema.vertexLabel(\"continent\").exists()";
            string createContinentVertexLabelStmt = "schema.vertexLabel(\"continent\").partitionKey(\"name\").create()";

            string existsCountryVertexLabelStmt = "schema.vertexLabel(\"country\").exists()";
            string createCountryVertexLabelStmt = "schema.vertexLabel(\"country\").partitionKey(\"name\").properties(\"capital\").create()";
   
            string existsHasCountryEdgeLabelStmt = "schema.edgeLabel(\"has_country\").exists()";
            string createHasCountryEdgeLabelStmt = "schema.edgeLabel(\"has_country\").connection(\"continent\", \"country\").create()";

            executeIfNotExists(session, existsGraphStmt, createGraphStmt);
            executeIfNotExists(session, existsCapitalPropertyKeyStmt, createCapitalPropertyStmt);
            executeIfNotExists(session, existsNamePropertyKeyStmt, createNamePropertyKeyStmt);
            executeIfNotExists(session, existsContinentVertexLabelStmt, createContinentVertexLabelStmt);
            executeIfNotExists(session, existsCountryVertexLabelStmt, createCountryVertexLabelStmt);
            executeIfNotExists(session, existsHasCountryEdgeLabelStmt, createHasCountryEdgeLabelStmt);

        }

        static void addContinentVertex(IDseSession session, GraphTraversalSource g, string continentName) {
            session.ExecuteGraph(DseGraph.StatementFromTraversal(g.AddV("continent").Property("name", continentName)));
        }

        static void addCountryVertex(IDseSession session, GraphTraversalSource g, string countryName, string capitalName) {
            session.ExecuteGraph(DseGraph.StatementFromTraversal(g.AddV("country").Property("name", countryName).Property("capital", capitalName)));
        }

        static void addHasCountryEdge(IDseSession session, GraphTraversalSource g, string continentName, string countryName) {
            session.ExecuteGraph(DseGraph.StatementFromTraversal(g.V().Has("continent", "name", continentName).AddE("has_country").To(g.V().Has("country", "name", countryName))));
        }

        static void Main(string[] args)
        {
            string graphName = "world_graph";
            IDseCluster cluster = DseCluster.Builder()
                                .AddContactPoint("127.0.0.1")
                                .WithGraphOptions(new GraphOptions().SetName(graphName))
                                .Build();
            IDseSession session = cluster.Connect();
            
            createSchema(session, graphName);

            GraphTraversalSource g = DseGraph.Traversal(session);

            addContinentVertex(session, g, "Europe");
            addContinentVertex(session, g, "Asia");
            addContinentVertex(session, g, "North America");

            addCountryVertex(session, g, "United Kingdom", "London");
            addCountryVertex(session, g, "Germany", "Berlin");
            addCountryVertex(session, g, "Spain", "Madrid");
            addCountryVertex(session, g, "China", "Beijing");
            addCountryVertex(session, g, "United States", "Washington DC");

            addHasCountryEdge(session, g, "Europe", "United Kingdom");
            addHasCountryEdge(session, g, "Europe", "Germany");
            addHasCountryEdge(session, g, "Europe", "Spain");
            addHasCountryEdge(session, g, "Asia", "China");
            addHasCountryEdge(session, g, "North America", "United States");

            GraphResultSet resultSet = session.ExecuteGraph(g.V().Has("continent", "name", "Europe").OutE("has_country").InV());

            foreach (IVertex vertex in resultSet.To<IVertex>()){
                Console.WriteLine($"The capital of European country {vertex.GetProperty("name").Value} is {vertex.GetProperty("capital").Value}");
            }

            cluster.Shutdown();
        }
    }
}




