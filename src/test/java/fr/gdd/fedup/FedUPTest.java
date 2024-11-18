package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.lib.Alg;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.VarFinder;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Testing the whole process from getting a query to the retrieving the results
 * over the federation of SPARQL endpoints.
 */
class FedUPTest {

    private static final Logger log = LoggerFactory.getLogger(FedUPTest.class);

    static InMemorySummaryFactory imsf;
    static Dataset dataset;
    static Summary summary;
    static Set<String> endpoints = Set.of("https://graphA.org", "https://graphB.org");

    @BeforeAll
    public static void initialize_dataset() {
        imsf = new InMemorySummaryFactory();
        dataset = imsf.getPetsDataset();
        summary = imsf.getSimplePetsSummary();
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
    public void a_query_with_only_one_tp () {
        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://something/that/doesnot/exist> ?o .
                }""";
        FedUP fedup = new FedUP(summary, dataset);
        String result = fedup.query(queryAsString, endpoints);
        // assertEquals("SELECT*WHERE{}", result.replace("\n", "").replace(" ", ""));
    }

    @Test
    public void simple_query_with_two_endpoints () {
        // Alice is a constant, so it gets actually checked using an ASK
        // and since only graphA has it, it means tp#1@A&B, and tp#2@A.
        checkQueryWithActualEndpoints("""
                SELECT * WHERE {
                    ?s <http://auth/named> ?o .
                    ?s <http://auth/named> <http://auth/Alice>
                }""");
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
        checkQueryWithActualEndpoints("""
                SELECT ?person ?animal WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL {
                        ?person <http://auth/owns> ?animal
                    }
                }""");
    }

    @Test
    public void every_person_with_its_OPTIONAL_animal_and_its_number () {
        checkQueryWithActualEndpoints("""
                SELECT ?person ?animal ?nb WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL { ?person <http://auth/owns> ?animal }
                    OPTIONAL { ?person <http://auth/nbPets> ?nb }
                }""");
    }

    @Test
    public void every_person_with_its_OPTIONAL_animal_and_its_number_when_the_animal_exists () {
        // Slightly different query than before. The OPTIONAL is nested inside the
        // OPTIONAL animal.
       checkQueryWithActualEndpoints("""
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL {
                        ?person <http://auth/owns> ?animal
                         OPTIONAL {
                            ?person <http://auth/nbPets> ?nb
                        }
                   }
                }""");
    }

    @Test
    public void retrieve_all_people_and_all_animals_with_a_union () {
        checkQueryWithActualEndpoints("""
                SELECT ?person ?any ?animal WHERE {
                    {<http://auth/person> <http://auth/named> ?person .}
                    UNION { ?any <http://auth/owns> ?animal }
                }""");
    }

    @Test
    public void retrieve_triples_from_disjoint_sources () {
        // Alice -> cat @A
        // David -> dog @B
        checkQueryWithActualEndpoints("""
                SELECT * WHERE {
                    {<http://auth/Alice> <http://auth/owns> ?cat .}
                    UNION { <http://auth/David> <http://auth/owns> ?dog }
                }""");
    }

    @Test
    public void query_with_a_limit_and_order_by () {
        // should only get Alice -> cat or David -> dog, but with ORDER BY
        // should only get Alice -> cat since "dog" is after "cat" in
        // lexicographical order.
        checkQueryWithActualEndpoints("""
                SELECT * WHERE {
                    ?people <http://auth/owns> ?animal
                } ORDER BY ?animal LIMIT 1
                """);
    }

    @Test
    public void query_with_a_projected_variable () {
        // should get Alice then David since it's order
        // by their respective animal.
        // TODO replace data structure so we can see the order
        // TODO of arrival of results.
        // TODO OR get next by next in the execution function
        checkQueryWithActualEndpoints("""
                SELECT ?people WHERE {
                    ?people <http://auth/owns> ?animal
                } ORDER BY ?animal
                """);
    }

    @Test
    public void query_with_twice_the_same_data () {
        // twice the same data
        checkQueryWithActualEndpoints("""
                SELECT * WHERE {
                    { ?people <http://auth/owns> ?animal }
                    UNION { ?people <http://auth/owns> ?animal }
                }""");
    }

    @Test
    public void query_with_a_distinct_to_remove_duplicates () {
        // twice the same data but the distinct removes duplicates
        checkQueryWithActualEndpoints("""
                SELECT DISTINCT * WHERE {
                    { ?people <http://auth/owns> ?animal }
                    UNION { ?people <http://auth/owns> ?animal }
                }""");
    }

    @Test
    public void query_with_a_filter_clause () {
        // remove one of the solution, keeping David and his dog.
        // however, endpointA still appears in the logical plan since
        // FedUP removes filter to operate.
        checkQueryWithActualEndpoints("""
                SELECT DISTINCT * WHERE {
                    { ?people <http://auth/owns> ?animal }
                    FILTER ( ?animal = <http://auth/dog> )
                }""");
    }

    @Test
    public void query_with_a_filter_clause_that_is_useless () {
        // remove all solution as ?meow is undefined. But it
        // still needs to be pushed down in both branches.
        checkQueryWithActualEndpoints("""
                SELECT DISTINCT * WHERE {
                    { ?people <http://auth/owns> ?animal }
                    FILTER ( ?meow = <http://auth/dog> )
                }""");
    }

    @Test
    public void query_with_a_conjunctive_filter_clause_should_be_split () {
        checkQueryWithActualEndpoints("""
                SELECT DISTINCT * WHERE {
                    { ?people <http://auth/owns> ?animal }
                    FILTER ( ?meow = <http://auth/dog> && ?animal = <http://auth/dog>)
                }""");
    }

    @Test
    public void query_with_a_conjunctive_filter_clause_should_be_split_and_one_as_not_both () {
        // remove one of the solution, keeping David and his dog.
        // however, endpointA still appears in the logical plan since
        // FedUP removes filter to operate.
        checkQueryWithActualEndpoints("""
                SELECT DISTINCT * WHERE {
                    { ?people <http://auth/owns> ?animal .
                      ?people  <http://auth/nbPets> ?nb
                    }
                    FILTER ( ?meow = <http://auth/dog> && ?animal = <http://auth/dog>)
                }""");
    }

    @Test
    public void tricky_query_with_two_optionals() {
        Dataset dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        List<String> statements = Arrays.asList(
                "<http://auth/Alice> <http://something/owns> <http://auth/dog>."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelA = ModelFactory.createDefaultModel();
        modelA.read(statementsStream, "", Lang.NT.getLabel());

        statements = Arrays.asList(
                "<http://auth/dog> <http://something/likes> <http://auth/Bob>.",
                "<http://auth/dog> <http://something/likes> <http://auth/Croquettes>.",
                "<http://auth/Bob> <http://something/race> \"French Poodle\".",
                "<http://auth/Croquettes> <http://something/category> <http://auth/A>."
        );
        statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelB = ModelFactory.createDefaultModel();
        modelB.read(statementsStream, "", Lang.NT.getLabel());

        dataset.addNamedModel("https://graphA.org", modelA);
        dataset.addNamedModel("https://graphB.org", modelB);

        dataset.commit();
        dataset.end();

        Summary summary = new Summary(new TransformCopy(), dataset);
        FedUP fedup = new FedUP(summary, dataset);
        String rsa = fedup.query("""
               SELECT * WHERE {
               ?p <http://something/owns> ?a .
               ?a <http://something/likes> ?s .
               OPTIONAL {?s <http://something/race> ?r}
               OPTIONAL {?s <http://something/category> ?c}
               }
               """);
        log.debug(rsa);
        // There should be two optionals in the final query since both "race" and
        // "category" exist.
        assertEquals(2, StringUtils.countMatches(rsa, "OPTIONAL"));
    }



    /* ********************************************************************** */

    /**
     * Factorize the code to execute locally and on remote endpoint to check
     * if everything is ok.
     * @param queryAsString The normal query to execute (not the service one).
     */
    public static void checkQueryWithActualEndpoints(String queryAsString) {
        // before, had to replace manually (see below), now we have a convenience function
        // `modifyEndpoints` to do that instead.
        FedUP fedup = new FedUP(summary, dataset)
                .shouldNotFactorize()
                .modifyEndpoints(e ->
                        e.contains("graphA") ?
                                "http://localhost:3333/graphA/sparql":
                                "http://localhost:3334/graphB/sparql"
                );

        String result = fedup.query(queryAsString, endpoints);

        // In the summary, they are placeholder, so we replace the value by the proper
        // In reality, the summary would have ingested the actual uri, so no problem.
        // String endpointA = "http://localhost:3333/graphA/sparql";
        // String endpointB = "http://localhost:3334/graphB/sparql";
        // result = result.replace("https://graphA.org", endpointA)
        //         .replace("https://graphB.org", endpointB);

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
                .add("graphA", imsf.getGraph("https://graphA.org"))
                .build();

        FusekiServer serverB = FusekiServer.create()
                .port(3334)
                .add("graphB", imsf.getGraph("https://graphB.org"))
                .build();

        serverA.start();
        serverB.start();

        return List.of(serverA, serverB);
    }

    public static void stopServers(List<FusekiServer> servers) {
        servers.forEach(s -> s.stop());
    }
}