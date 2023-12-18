package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.federated.repository.FedXRepositoryConnection;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
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
    public void fedup_to_fedx_with_optional() {
        executeFedUPThenFedX("""
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL { ?person <http://auth/owns> ?animal }
                }""");
    }

    @Test
    public void fedup_to_fedx_with_filter_to_see_if_they_are_pushed_down() {
        executeFedUPThenFedX("""
            SELECT DISTINCT * WHERE {
            { ?people <http://auth/owns> ?animal }
              FILTER ( ?people = <http://auth/dog> )
            }""");
    }

    @Test
    public void fedx_with_filter() {
        // filters are effectively pushed down. BUT limit and distinct aren't…
        executeWithFedX("""
                SELECT DISTINCT  *
                WHERE
                  {   { SERVICE SILENT <http://localhost:3333/graphA/sparql>
                          { ?people  <http://auth/owns>  ?animal}
                      }
                    UNION
                      { SERVICE SILENT <http://localhost:3334/graphB/sparql>
                          { ?people  <http://auth/owns>  ?animal}
                      }
                    FILTER ( ?animal = <http://auth/dog> )
                  }
                  """);
        // QueryRoot
        //   Distinct
        //      Projection
        //         ProjectionElemList
        //            ProjectionElem "people"
        //            ProjectionElem "animal"
        //         NUnion
        //            ExclusiveStatement
        //               Var (name=people)
        //               Var (name=_const_4ea587e2_uri, value=http://auth/owns, anonymous)
        //               Var (name=animal, value=http://auth/dog)
        //               StatementSource (id=sparql_localhost:3333_graphA_sparql, type=REMOTE)
        //               BoundFilters (animal=http://auth/dog)
        //            ExclusiveStatement
        //               Var (name=people)
        //               Var (name=_const_4ea587e2_uri, value=http://auth/owns, anonymous)
        //               Var (name=animal, value=http://auth/dog)
        //               StatementSource (id=sparql_localhost:3334_graphB_sparql, type=REMOTE)
        //               BoundFilters (animal=http://auth/dog)
    }

    /* ***************************************************************** */

    public void executeFedUPThenFedX(String queryAsString) {
        FedUP fedup = new FedUP(summary, dataset).shouldNotFactorize();
        String result = fedup.query(queryAsString, new HashSet<>(graphs));
        // so we need to replace
        result = result.replace(graphs.get(0), endpoints.get(0))
                .replace(graphs.get(1), endpoints.get(1));
        executeWithFedX(result);
    }


    public void executeWithFedX(String queryAsString) {
        // still need dataset since the dataset refers to <graphA> and not
        // endpoint remote address.
        List<FusekiServer> servers = FedUPTest.startServers();

        FedXConfig config = new FedXConfig();
        config.withDebugQueryPlan(true); // in std out…
        FedXRepository fedx = FedXFactory.newFederation()
                .withConfig(config)
                .withSparqlEndpoints(endpoints).create();

        try (FedXRepositoryConnection conn = fedx.getConnection()) {
            TupleQuery tq = conn.prepareTupleQuery(queryAsString);

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
