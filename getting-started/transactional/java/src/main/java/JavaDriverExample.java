import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class JavaDriverExample {
    
    public static void main(String[] args){
        String keyspace = "world_ks";
        String table = "world_tbl";

        DseCluster cluster = DseCluster.builder().addContactPoints("127.0.0.1").build();
        DseSession session = cluster.connect();
        
        SimpleStatement createKeyspaceStmt = new SimpleStatement(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};", keyspace));
        SimpleStatement createTableStmt = new SimpleStatement(String.format("CREATE TABLE IF NOT EXISTS %s.%s (continent text, country text, capital text, PRIMARY KEY (continent, country))", keyspace, table));
        
        session.execute(createKeyspaceStmt);
        session.execute(createTableStmt);

        PreparedStatement insertStmt = session.prepare(String.format("INSERT INTO %s.%s (continent, country, capital) VALUES (?,?,?)", keyspace, table));

        // Write 5 rows
        session.execute(insertStmt.bind("Europe", "United Kingdom", "London"));
        session.execute(insertStmt.bind("Europe", "Germany", "Berlin"));
        session.execute(insertStmt.bind("Europe", "Spain", "Madrid"));
        session.execute(insertStmt.bind("Asia", "China", "Beijing"));
        session.execute(insertStmt.bind("North America", "United States", "Washington DC"));

        SimpleStatement readStmt = new SimpleStatement(String.format("SELECT country, capital FROM %s.%s WHERE continent = 'Europe'", keyspace, table));
        
        // Read the European countries and their capitals
        ResultSet rs = session.execute(readStmt);

        for (Row row : rs) {
            System.out.println("The capital of European country " + row.getString("country") + " is " + row.getString("capital"));
        }

        cluster.close();
    }
}