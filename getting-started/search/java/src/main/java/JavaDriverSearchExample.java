import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.dse.geometry.Point;

public class JavaDriverSearchExample {
    
    public static void main(String[] args){
        String keyspace = "world_ks";
        String table = "world_search_tbl";

        DseCluster cluster = DseCluster.builder().addContactPoints("127.0.0.1").build();
        DseSession session = cluster.connect();
        
        SimpleStatement createKeyspaceStmt = new SimpleStatement(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};", keyspace));
        SimpleStatement createTableStmt = new SimpleStatement(String.format("CREATE TABLE IF NOT EXISTS %s.%s (continent text, country text, capital text, location 'PointType', PRIMARY KEY (continent, country));", keyspace, table));
        SimpleStatement createSearchIndex = new SimpleStatement(String.format("CREATE SEARCH INDEX IF NOT EXISTS ON %s.%s WITH COLUMNS location;", keyspace, table));
    
        session.execute(createKeyspaceStmt);
        session.execute(createTableStmt);
        session.execute(createSearchIndex);

        PreparedStatement insertStmt = session.prepare(String.format("INSERT INTO %s.%s (continent, country, capital, location) VALUES (?,?,?,?)", keyspace, table));

        session.execute(insertStmt.bind("Europe", "United Kingdom", "London", new Point(51.5074, 0.1278)));
        session.execute(insertStmt.bind("Europe", "Germany", "Berlin", new Point(52.5200, 13.4050)));
        session.execute(insertStmt.bind("Europe", "Spain", "Madrid", new Point(40.4168, 3.7038)));
        session.execute(insertStmt.bind("Asia", "China", "Beijing", new Point(39.9042, 116.4074)));
        session.execute(insertStmt.bind("North America", "United States", "Washington DC", new Point(38.9072, 77.0369)));

        String solrQuery = "{ \"q\":\"*:*\", \"fq\":\"location:\\\"IsWithin(BUFFER(POINT(45 0), 20.0))\\\"\" }";
        SimpleStatement readStmt = new SimpleStatement(String.format("SELECT country, capital FROM %s.%s WHERE solr_query = '%s'", keyspace, table, solrQuery));
        
        ResultSet rs = session.execute(readStmt);

        for (Row row : rs) {
            System.out.println("The capital of European country " + row.getString("country") + " is " + row.getString("capital"));
        }

        cluster.close();
    }
}