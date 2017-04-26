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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

// A few assumptions are made, firstly that cassandra is listening
// to 127.0.0.1 (localhost) and that the native transport is listening
// to port 9042.
public class SessionManager
{
    private static SessionManager instance = null;
    private final Session session;
    private final Cluster cluster;
    private static final String host = "192.168.1.112";

    protected SessionManager()
    {
        cluster = Cluster.builder().addContactPoints(host).build();
        session = cluster.connect();
    }

    public static SessionManager getInstance()
    {
        return instance == null ? new SessionManager() : instance;
    }

    public Session getSession()
    {
        return session;
    }

    public static void closeSession()
    {
        if (instance == null)
            throw new RuntimeException("Error closing session as it's not initialized.");

        instance.session.close();
        System.out.println("Closed session successfully.");
    }
}
