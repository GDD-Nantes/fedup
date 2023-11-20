package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Set;

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
        // TODO assert
    }




}