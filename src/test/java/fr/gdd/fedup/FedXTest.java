package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;

/**
 * Testing another federated query engine, namely FedX. FedX is meant
 * for federated query execution and therefore benefits from some
 * optimizations.
 */
public class FedXTest {

    Logger log = LoggerFactory.getLogger(FedXTest.class);

    static Dataset dataset;
    static Summary summary;
    static List<String> graphs = List.of("https://graphA.org", "https://graphB.org");
    static List<String> endpoints = List.of("http://localhost:3333/graphA/sparql", "http://localhost:3334/graphB/sparql");


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
    public void fedx_query_with_optional() {
        executeWithFedX("""
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL { ?person <http://auth/owns> ?animal }
                }""");
    }

    /* ***************************************************************** */

    public void executeWithFedX(String queryAsString) {
        // still need dataset since the dataset refers to <graphA> and not
        // endpoint remote address.
        FedUP fedup = new FedUP(summary, dataset);
        String result = fedup.query(queryAsString, new HashSet<>(graphs));

        // so we need to replace
        result = result.replace(graphs.get(0), endpoints.get(0))
                .replace(graphs.get(1), endpoints.get(1));

        List<FusekiServer> servers = FedUPTest.startServers();

        FedXConfig config = new FedXConfig();
        config.withDebugQueryPlan(true); // in std out…
        Repository fedx = FedXFactory.newFederation()
                .withConfig(config)
                .withSparqlEndpoints(endpoints).create();

        try (RepositoryConnection conn = fedx.getConnection()) {
            TupleQuery tq = conn.prepareTupleQuery(result);

            try (TupleQueryResult tqRes = tq.evaluate()) {

                int count = 0;
                while (tqRes.hasNext()) {
                    BindingSet b = tqRes.next();
                    log.debug(b.toString());
                    count++;
                }

                log.debug("Got {} results.", count);
            }

        }

        FedUPTest.stopServers(servers);
    }
}
