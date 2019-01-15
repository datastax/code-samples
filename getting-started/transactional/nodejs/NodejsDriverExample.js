`use strict`;

const dse = require('dse-driver');

async function run() {
    
    const client = new dse.Client({ contactPoints: ["127.0.0.1"], localDataCenter: "datacenter1" });

    const keyspace = "world_ks";
    const table = "world_tbl";
    
    const createKeyspaceStmt = `CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};`;
    const createTableStmt = `CREATE TABLE IF NOT EXISTS ${keyspace}.${table} (continent text, country text, capital text, PRIMARY KEY (continent, country));`;
    
    await client.execute(createKeyspaceStmt);
    await client.execute(createTableStmt);

    const insertStmt = `INSERT INTO ${keyspace}.${table} (continent, country, capital) VALUES (?,?,?);`;

    await client.execute(insertStmt, ["Europe", "United Kingdom", "London"], { prepare: true } );
    await client.execute(insertStmt, ["Europe", "Germany", "Berlin"], { prepare: true } );
    await client.execute(insertStmt, ["Europe", "Spain", "Madrid"], { prepare: true } );
    await client.execute(insertStmt, ["Asia", "China", "Beijing"], { prepare: true } );
    await client.execute(insertStmt, ["North America", "United States", "Washington DC"], { prepare: true } );

    const readStmt = `SELECT country, capital FROM ${keyspace}.${table} WHERE continent = 'Europe'`;

    await client.execute(readStmt).then(result => {
        for (let row of result ) {
            console.log("The capital of European country " + row.country + " is " + row.capital);
        }
    });

    await client.shutdown();
}

run();