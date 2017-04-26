/*    Copyright 2014 DataStax
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.*;

public class SimpleCRUD
{
    private Session session;
    private static final String KEYSPACE = "music";
    private static final String TABLE = "song";

    public SimpleCRUD()
    {
        session = SessionManager.getInstance().getSession();
    }

    private void initSchema()
    {
        // create the keyspace for this example, using IF NOT EXISTS allows us to try
        // creating the keyspace without having to check whether it already exists
        String initKS = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
        session.execute(String.format(initKS, KEYSPACE));

        // again IF NOT EXISTS is used to save us the hassle of having to check
        // whether the table is already defined.
        String initTbl = "CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, title text, date_added timeuuid, tags set<text>);";
        session.execute(String.format(initTbl, KEYSPACE, TABLE));
    }

    private void create()
    {
        // the below insert statement contains an example of CQL functions (the example being uuid()
        // now() which generates a UUID but of type 1. see:
        // http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/timeuuid_functions_r.html)

        // mannually insert the first partition
        // QueryBuilder.raw(...) is called to avoid escaping the CQL3 functions
        Insert insert = QueryBuilder.insertInto(KEYSPACE, TABLE);
        insert.value("id", UUID.fromString("f8053760-138f-11e4-843c-0002a5d5c51b"))
              .value("title", "Now it's Missing")
              .value("date_added", QueryBuilder.raw(" now()"))
              .value("tags", QueryBuilder.raw("{'balade', 'rock'}"));
        session.execute(insert);

        // insert another 999 random (based on the uuid() function) partitions where we wont know
        // the id of the partition until it's inserted.
        for (int i = 0; i < 999; i++)
        {
            Insert insertRand = QueryBuilder.insertInto(KEYSPACE, TABLE);
            String tags = String.format("{'%s', '%s'}", RandomStringUtils.randomAlphabetic(5), RandomStringUtils.randomAlphabetic(5));
            insertRand.value("id", QueryBuilder.raw("uuid()"))
                      .value("title", RandomStringUtils.random(20))
                      .value("date_added", QueryBuilder.raw("now()"))
                      .value("tags", QueryBuilder.raw(tags));
            session.execute(insertRand);
        }
    }

    private void read()
    {
        // select 20 partitions we've entered
        Select select = QueryBuilder.select()
                                    .all()
                                    .from(KEYSPACE, TABLE)
                                    .limit(20);
        ResultSet rs = session.execute(select);
        // display selected rows
        for(Row r : rs.all())
            System.out.println(r.toString());

        // Select a single partition by id
        Clause byId = QueryBuilder.eq("id", UUID.fromString("f8053760-138f-11e4-843c-0002a5d5c51b"));
        Select.Where selectOne = QueryBuilder.select()
                                             .all()
                                             .from(KEYSPACE, TABLE)
                                             .where(byId);
        ResultSet rsOne = session.execute(selectOne);
        // display the selected partition
        System.out.println(rsOne.one().toString() + " Of total rows: " + rs.all().size());

        // Select only two fields from a partition.
        Select selectTwoFields = QueryBuilder.select()
                                             .column("id")
                                             .column("date_added")
                                             .from(KEYSPACE, TABLE)
                                             .limit(10);
        ResultSet rsTwoFields = session.execute(selectTwoFields);
        // display the selected partition
        for (Row r : rsTwoFields)
            System.out.println(r.toString());
    }

    private void update()
    {
        // for Cassandra updates are the same as inserts, see:
        // http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_about_inserts_c.html
        // that being said, CQL3 does have syntax specific to updates:
        Clause byId = QueryBuilder.eq("id", QueryBuilder.raw("f8053760-138f-11e4-843c-0002a5d5c51b"));
        //  = QueryBuilder.update(KEYSPACE, TABLE)..where(byId);

        Update.Where update = QueryBuilder.update(KEYSPACE, TABLE)
                                          .with(QueryBuilder.set("title", "The Awesome Song"))
                                          .where(byId);
        session.execute(update);

        // on the fly verification of the update.
        String selectUpdate = String.format("SELECT * FROM %s.%s WHERE id=%s", KEYSPACE, TABLE, "f8053760-138f-11e4-843c-0002a5d5c51b");
        System.out.println(session.execute(selectUpdate).all());
    }

    private void delete()
    {
        Clause byId = QueryBuilder.eq("id", UUID.fromString("f8053760-138f-11e4-843c-0002a5d5c51b"));
        Delete delete = QueryBuilder.delete().from(KEYSPACE, TABLE);
        delete.where(byId);
        session.execute(delete);
        System.out.println("Deleted 'f8053760-138f-11e4-843c-0002a5d5c51b'.");

        // on the fly verificatio of the delete
        String query = String.format("SELECT count(*) FROM %s.%s", KEYSPACE, TABLE);
        // get the results by using the first row, and then get the count value (get first long)
        System.out.println("1000 - 1 = " + session.execute(query).all().get(0).getLong(0));
    }

    public static void main(String [] args)
    {
        SimpleCRUD simpleCRUD = new SimpleCRUD();
        simpleCRUD.initSchema();
        simpleCRUD.create();
        simpleCRUD.read();
        simpleCRUD.update();
        simpleCRUD.delete();
    }
}
