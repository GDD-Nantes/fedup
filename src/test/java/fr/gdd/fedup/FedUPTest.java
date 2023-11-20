package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.*;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FedUPTest {

    Logger log = LoggerFactory.getLogger(FedUPTest.class);

    static Dataset dataset;
    static Summary summary;
    static Set<String> endpoints = Set.of("https://graphA.org", "https://graphB.org");

    @BeforeAll
    public static void initialize_dataset() {
        dataset = InMemorySummaryFactory.getPetsDataset();
        summary = InMemorySummaryFactory.getSimplePetsSummary();
    }

    @AfterAll
    public static void drop_dataset() {
        TDBInternal.expel(dataset.asDatasetGraph());
        TDBInternal.expel(summary.getSummary().asDatasetGraph());
    }

    @Test
    public void a_query_that_returns_nothing_should_generate_an_empty_sparql_query () {
        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://something/that/doesnot/exist> ?o .
                    ?s <http://auth/named> <http://auth/Alice>
                }""";
        FedUP fedup = new FedUP(summary, dataset);
        String result = fedup.query(queryAsString, endpoints);
        assertEquals("SELECT*WHERE{}", result.replace("\n", "").replace(" ", ""));
    }

    @Test
    public void simple_query_with_two_endpoints () {
        // Alice is a constant, so it gets actually checked using an ASK
        // and since only graphA has it, it means tp#1@A&B, and tp#2@A.
        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/named> ?o .
                    ?s <http://auth/named> <http://auth/Alice>
                }""";
        FedUP fedup = new FedUP(summary, dataset);

        String result = fedup.query(queryAsString, endpoints);
        log.debug(result);

        // create the server
        FusekiServer serverA = FusekiServer.create()
                .port(3333)
                .add("graphA", InMemorySummaryFactory.getGraph("https://graphA.org"))
                .build();

        FusekiServer serverB = FusekiServer.create()
                .port(3334)
                .add("graphB", InMemorySummaryFactory.getGraph("https://graphB.org"))
                .build();

        String endpointA = "http://localhost:3333/graphA/sparql";
        String endpointB = "http://localhost:3334/graphB/sparql";

        // In the summary, they are placeholder, so we replace the value by the proper
        // In reality, the summary would have ingested the actual uri, so no problem.
        result = result.replace("https://graphA.org", endpointA)
                .replace("https://graphB.org", endpointB);

        serverA.start();
        serverB.start();

        // DatasetFactory.empty() otherwise the poor Jena Engine looses its mind.
        try (QueryExecution qe = QueryExecutionFactory.create(result, DatasetFactory.empty())) {
            ResultSet results = qe.execSelect();
            int nbResults = 0;
            while (results.hasNext()) {
                log.debug(results.next().toString());
                ++nbResults;
            }
            assertEquals(4, nbResults); // alice, bob @ graphA; carol, david @ graphB
        }

        serverA.stop();
        serverB.stop();
    }
}