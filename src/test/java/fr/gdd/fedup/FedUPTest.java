package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FedUPTest {

    private static Logger log = LoggerFactory.getLogger(FedUPTest.class);

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

        // In the summary, they are placeholder, so we replace the value by the proper
        // In reality, the summary would have ingested the actual uri, so no problem.
        String endpointA = "http://localhost:3333/graphA/sparql";
        String endpointB = "http://localhost:3334/graphB/sparql";
        result = result.replace("https://graphA.org", endpointA)
                .replace("https://graphB.org", endpointB);

        List<FusekiServer> servers = startServers();
        log.debug("Results are {}", equalExecutionResults(queryAsString, result, dataset));
        stopServers(servers);
    }

    @Test
    public void every_person_with_its_OPTIONAL_animal () {
        // The expected FedQPL expression is:
        // Mu { @1 Lj { Mu @1 @2 }, @2 Lj { Mu @1 @2 } }
        // However, the assignments provides all 4 combinations, which leads
        // to wrong results with naive logical plan:
        // Mu { @1 Lj @1, @1 Lj @2, @2 Lj @2, @2 Lj @1 }
        // Among others, Bob does not own a pet, which is true in @1 Lj @1 and
        // @1 Lj @2, therefore, the prefix is repeated which leads to wrong results.

        String queryAsString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL {
                        ?person <http://auth/owns> ?animal
                    }
                }""";
        FedUP fedup = new FedUP(summary, dataset);

        String result = fedup.query(queryAsString, endpoints);

        // In the summary, they are placeholder, so we replace the value by the proper
        // In reality, the summary would have ingested the actual uri, so no problem.
        String endpointA = "http://localhost:3333/graphA/sparql";
        String endpointB = "http://localhost:3334/graphB/sparql";
        result = result.replace("https://graphA.org", endpointA)
                .replace("https://graphB.org", endpointB);

        List<FusekiServer> servers = startServers();
        log.debug("Results are {}", equalExecutionResults(queryAsString, result, dataset));
        stopServers(servers);
    }

    @Test
    public void every_person_with_its_OPTIONAL_animal_and_its_number () {
        String queryAsString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL {
                        ?person <http://auth/owns> ?animal
                    }
                    OPTIONAL {
                        ?person <http://auth/nbPets> ?nb
                    }
                }""";
        FedUP fedup = new FedUP(summary, dataset);

        String result = fedup.query(queryAsString, endpoints);

        // In the summary, they are placeholder, so we replace the value by the proper
        // In reality, the summary would have ingested the actual uri, so no problem.
        String endpointA = "http://localhost:3333/graphA/sparql";
        String endpointB = "http://localhost:3334/graphB/sparql";
        result = result.replace("https://graphA.org", endpointA)
                .replace("https://graphB.org", endpointB);

        List<FusekiServer> servers = startServers();
        log.debug("Results are {}", equalExecutionResults(queryAsString, result, dataset));
        stopServers(servers);
    }

    @Test
    public void every_person_with_its_OPTIONAL_animal_and_its_number_when_the_animal_exists () {
        // Slightly different query than before. The OPTIONAL is nested inside the
        // OPTIONAL animal.
        String queryAsString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL {
                        ?person <http://auth/owns> ?animal
                         OPTIONAL {
                            ?person <http://auth/nbPets> ?nb
                        }
                   }
                }""";
        FedUP fedup = new FedUP(summary, dataset);

        String result = fedup.query(queryAsString, endpoints);

        // In the summary, they are placeholder, so we replace the value by the proper
        // In reality, the summary would have ingested the actual uri, so no problem.
        String endpointA = "http://localhost:3333/graphA/sparql";
        String endpointB = "http://localhost:3334/graphB/sparql";
        result = result.replace("https://graphA.org", endpointA)
                .replace("https://graphB.org", endpointB);

        List<FusekiServer> servers = startServers();
        log.debug("Results are {}", equalExecutionResults(queryAsString, result, dataset));
        stopServers(servers);
    }


    /* ********************************************************************** */

    public static MultiSet<Binding> equalExecutionResults(String originalQuery, String serviceQuery, Dataset dataset) {
        MultiSet<Binding> originalResults = new HashMultiSet<>();

        Dataset union = DatasetFactory.create();
        union.begin(ReadWrite.WRITE);
        dataset.begin(ReadWrite.READ);
        union.setDefaultModel(dataset.getUnionModel());
        dataset.commit();
        dataset.end();
        union.commit();
        union.end();

        union.begin(ReadWrite.READ);
        try (QueryExecution qe =  QueryExecutionFactory.create(originalQuery, union)) {
            ResultSet results = qe.execSelect();
            while (results.hasNext()) {
                originalResults.add(results.nextBinding());
            }
        }
        union.commit();
        union.end();

        MultiSet<Binding> serviceResults = new HashMultiSet<>();
        try (QueryExecution qe =  QueryExecutionFactory.create(serviceQuery, DatasetFactory.empty())) {
            ResultSet results = qe.execSelect();
            while (results.hasNext()) {
                serviceResults.add(results.nextBinding());
            }
        }

        log.debug("Original size: {}", originalResults.size());
        log.debug("Service  size: {}", serviceResults.size());
        assertEquals(originalResults, serviceResults);
        return serviceResults;
    }

    /* ***************************************************************** */

    public static List<FusekiServer> startServers() {
        // create the server
        FusekiServer serverA = FusekiServer.create()
                .port(3333)
                .add("graphA", InMemorySummaryFactory.getGraph("https://graphA.org"))
                .build();

        FusekiServer serverB = FusekiServer.create()
                .port(3334)
                .add("graphB", InMemorySummaryFactory.getGraph("https://graphB.org"))
                .build();

        serverA.start();
        serverB.start();

        return List.of(serverA, serverB);
    }

    public static void stopServers(List<FusekiServer> servers) {
        servers.forEach(s -> s.stop());
    }
}