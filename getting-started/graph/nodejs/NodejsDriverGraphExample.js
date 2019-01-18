`use strict`;

const dse = require('dse-driver');
const dseGraph = require('dse-graph');

async function executeIfNotExists(client, existsStmt, createStmt) {
    const result = await client.executeGraph(existsStmt, null, { executionProfile: 'schema' });
    if (String(result.first()).toLowerCase() === "false"){
        await client.executeGraph(createStmt, null, { executionProfile: 'schema' });
    }
}

async function createSchema(client, graphName) {

    const existsGraphStmt = `system.graph(\"${graphName}\").exists()`;
    const createGraphStmt = `system.graph(\"${graphName}\").create()`;

    const existsCapitalPropertyKeyStmt = `schema.propertyKey("capital").exists()`;
    const createCapitalPropertyStmt = `schema.propertyKey("capital").Text().single().create()`;

    const existsNamePropertyKeyStmt = `schema.propertyKey("name").exists()`;
    const createNamePropertyKeyStmt = `schema.propertyKey("name").Text().single().create()`;

    const existsContinentVertexLabelStmt = `schema.vertexLabel("continent").exists()`;
    const createContinentVertexLabelStmt = `schema.vertexLabel("continent").partitionKey("name").create()`;

    const existsCountryVertexLabelStmt = `schema.vertexLabel("country").exists()`;
    const createCountryVertexLabelStmt = `schema.vertexLabel("country").partitionKey("name").properties("capital").create()`;
   
    const existsHasCountryEdgeLabelStmt = `schema.edgeLabel("has_country").exists()`;
    const createHasCountryEdgeLabelStmt = `schema.edgeLabel("has_country").connection("continent", "country").create()`;

    await executeIfNotExists(client, existsGraphStmt, createGraphStmt);
    await executeIfNotExists(client, existsCapitalPropertyKeyStmt, createCapitalPropertyStmt);
    await executeIfNotExists(client, existsNamePropertyKeyStmt, createNamePropertyKeyStmt);
    await executeIfNotExists(client, existsContinentVertexLabelStmt, createContinentVertexLabelStmt);
    await executeIfNotExists(client, existsCountryVertexLabelStmt, createCountryVertexLabelStmt);
    await executeIfNotExists(client, existsHasCountryEdgeLabelStmt, createHasCountryEdgeLabelStmt);
}

async function addContinentVertex(client, g, continentName) {
    await client.executeGraph(dseGraph.queryFromTraversal(g.addV('continent').property('name', continentName)));
}

async function addCountryVertex(client, g, countryName, capitalName) {
    await client.executeGraph(dseGraph.queryFromTraversal(g.addV("country").property("name", countryName).property("capital", capitalName)));
}

async function addHasCountryEdge(client, g, continentName, countryName) {
    await client.executeGraph(dseGraph.queryFromTraversal(g.V().has("continent", "name", continentName).addE("has_country").to(g.V().has("country", "name", countryName))));
}

async function run() {
    
    const graphName = "world_graph";
    const client = new dse.Client({ 
        contactPoints: ["127.0.0.1"], 
        localDataCenter: "datacenter1", 
        profiles: [ 
            new dse.ExecutionProfile('default', {graphOptions: { name: graphName, language: 'bytecode-json'}}),
            new dse.ExecutionProfile('schema', {graphOptions: { name: graphName, language: 'gremlin-groovy'}}) ]}
        );

    
    await createSchema(client, graphName);

    const g = await dseGraph.traversalSource(client);

    await addContinentVertex(client, g, "Europe");
    await addContinentVertex(client, g, "Asia");
    await addContinentVertex(client, g, "North America");

    await addCountryVertex(client, g, "United Kingdom", "London");
    await addCountryVertex(client, g, "Germany", "Berlin");
    await addCountryVertex(client, g, "Spain", "Madrid");
    await addCountryVertex(client, g, "China", "Beijing");
    await addCountryVertex(client, g, "United States", "Washington DC");

    await addHasCountryEdge(client, g, "Europe", "United Kingdom");
    await addHasCountryEdge(client, g, "Europe", "Germany");
    await addHasCountryEdge(client, g, "Europe", "Spain");
    await addHasCountryEdge(client, g, "Asia", "China");
    await addHasCountryEdge(client, g, "North America", "United States");

    const query = dseGraph.queryFromTraversal(g.V().has("continent", "name", "Europe").outE("has_country").inV());

    await client.executeGraph(query).then(result => {
        for (let row of result ) {
            console.log("The capital of European country " + row.properties['name'][0].value + " is " + row.properties['capital'][0].value)
        }
    });

    await client.shutdown();
}

run();