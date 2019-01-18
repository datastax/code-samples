`use strict`;

const dse = require('dse-driver');

async function run() {
    
    const client = new dse.Client({ contactPoints: ["127.0.0.1"], localDataCenter: "datacenter1" });
    
    const keyspace = "world_ks";
    const table = "world_search_tbl";
    
    const Point = dse.geometry.Point;

    const createKeyspaceStmt = `CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};`;
    const createTableStmt = `CREATE TABLE IF NOT EXISTS ${keyspace}.${table} (continent text, country text, capital text, location 'PointType', PRIMARY KEY (continent, country));`;
    const createSearchIndexStmt = `CREATE SEARCH INDEX IF NOT EXISTS ON ${keyspace}.${table} WITH COLUMNS location;`;

    await client.execute(createKeyspaceStmt);
    await client.execute(createTableStmt);
    await client.execute(createSearchIndexStmt);

    const insertStmt = `INSERT INTO ${keyspace}.${table} (continent, country, capital, location) VALUES (?,?,?,?);`;

    await client.execute(insertStmt, ["Europe", "United Kingdom", "London", new Point(51.5074, 0.1278)], { prepare: true } );
    await client.execute(insertStmt, ["Europe", "Germany", "Berlin", new Point(52.5200, 13.4050)], { prepare: true } );
    await client.execute(insertStmt, ["Europe", "Spain", "Madrid", new Point(40.4168, 3.7038)], { prepare: true } );
    await client.execute(insertStmt, ["Asia", "China", "Beijing", new Point(39.9042, 116.4074)], { prepare: true } );
    await client.execute(insertStmt, ["North America", "United States", "Washington DC", new Point(38.9072, 77.0369)], { prepare: true } );

    const solrQuery = "{ \"q\":\"*:*\", \"fq\":\"location:\\\"IsWithin(BUFFER(POINT(45 0), 20.0))\\\"\" }";
    const readStmt = `SELECT country, capital FROM ${keyspace}.${table} WHERE solr_query = '${solrQuery}'`;

    await client.execute(readStmt).then(result => {
        for (let row of result ) {
            console.log("The capital of European country " + row.country + " is " + row.capital);
        }
    });

    await client.shutdown();
}

run();