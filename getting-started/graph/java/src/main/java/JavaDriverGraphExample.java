import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

import com.datastax.dse.graph.api.DseGraph;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.Vertex;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class JavaDriverGraphExample {

    private static void createSchema(DseSession session, String graphName){

        String existsGraphStmt = String.format("system.graph(\"%s\").exists()", graphName);
        String createGraphStmt = String.format("system.graph(\"%s\").create()", graphName);

        String existsCapitalPropertyKeyStmt = "schema.propertyKey(\"capital\").exists()";
        String createCapitalPropertyStmt = "schema.propertyKey(\"capital\").Text().single().create()";

        String existsNamePropertyKeyStmt = "schema.propertyKey(\"name\").exists()";
        String createNamePropertyKeyStmt = "schema.propertyKey(\"name\").Text().single().create()";

        String existsContinentVertexLabelStmt = "schema.vertexLabel(\"continent\").exists()";
        String createContinentVertexLabelStmt = "schema.vertexLabel(\"continent\").partitionKey(\"name\").create()";

        String existsCountryVertexLabelStmt = "schema.vertexLabel(\"country\").exists()";
        String createCountryVertexLabelStmt = "schema.vertexLabel(\"country\").partitionKey(\"name\").properties(\"capital\").create()";
       
        String existsHasCountryEdgeLabelStmt = "schema.edgeLabel(\"has_country\").exists()";
        String createHasCountryEdgeLabelStmt = "schema.edgeLabel(\"has_country\").connection(\"continent\", \"country\").create()";

        executeIfNotExists(session, existsGraphStmt, createGraphStmt);
        executeIfNotExists(session, existsCapitalPropertyKeyStmt, createCapitalPropertyStmt);
        executeIfNotExists(session, existsNamePropertyKeyStmt, createNamePropertyKeyStmt);
        executeIfNotExists(session, existsContinentVertexLabelStmt, createContinentVertexLabelStmt);
        executeIfNotExists(session, existsCountryVertexLabelStmt, createCountryVertexLabelStmt);
        executeIfNotExists(session, existsHasCountryEdgeLabelStmt, createHasCountryEdgeLabelStmt);

    }

    private static void executeIfNotExists(DseSession session, String existsStmt, String createStmt){
        GraphResultSet existsResultSet = session.executeGraph(existsStmt);
        if (existsResultSet.one().asString().equals("false")) {
            session.executeGraph(createStmt);
        }
    }

    private static void addContinentVertex(DseSession session, GraphTraversalSource g, String continentName){
        session.executeGraph(DseGraph.statementFromTraversal(g.addV("continent").property("name", continentName)));
    }

    private static void addCountryVertex(DseSession session, GraphTraversalSource g, String countryName, String capitalName){
        session.executeGraph(DseGraph.statementFromTraversal(g.addV("country").property("name", countryName).property("capital", capitalName)));
    }

    private static void addHasCountryEdge(DseSession session, GraphTraversalSource g, String continentName, String countryName){
        session.executeGraph(DseGraph.statementFromTraversal(g.addE("has_country").from(g.V().has("continent", "name", continentName)).to(g.V().has("country", "name", countryName))));
    }

    public static void main(String[] args){
        String graphName = "world_graph";

        DseCluster cluster = DseCluster.builder().addContactPoints("10.200.176.121").build();
        DseSession session = cluster.connect();
        GraphTraversalSource g = DseGraph.traversal();
        session.getCluster().getConfiguration().getGraphOptions().setGraphName(graphName);

        createSchema(session, graphName);

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

        GraphResultSet rs = session.executeGraph(DseGraph.statementFromTraversal(g.V().has("continent", "name", "Europe").outE("has_country").inV()));

        for (GraphNode node : rs) {
            Vertex vertex = node.asVertex();
            System.out.println("The capital of European country " + vertex.getProperty("name").getValue() + " is " + vertex.getProperty("capital").getValue());
        }

        cluster.close();
    }
}