package fr.gdd.fedup.transforms;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ToSourceSelectionTransformsTest {

    Logger log = LoggerFactory.getLogger(ToSourceSelectionTransformsTest.class);

    static Dataset dataset;
    static Summary summary;
    static Set<String> endpoints = Set.of("https://graphA.org", "https://graphB.org");

    @BeforeAll
    public static void initialize_dataset() {
        InMemorySummaryFactory imsf = new InMemorySummaryFactory();
        dataset = imsf.getPetsDataset();
        dataset.begin(ReadWrite.READ);
        summary = imsf.getSimplePetsSummary();
        summary.getSummary().begin(ReadWrite.READ);
    }

    @AfterAll
    public static void drop_dataset() {
        dataset.commit();
        // dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
        summary.getSummary().commit();
        // summary.getSummary().abort();
        TDBInternal.expel(summary.getSummary().asDatasetGraph());
    }

    @Disabled("Not up-to-date test")
    @Test
    public void adds_values_and_graph_closes() {
        // stupid query where the second triple pattern should be the first
        // one, since Alice is a constant.
        // In addition, we expect a values with only one graph since Alice exists
        // only at `graphA`.
        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/named> ?o .
                    ?s <http://auth/named> <http://auth/Alice>
                }
                """;
        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);

        ToSourceSelectionTransforms transforms = new ToSourceSelectionTransforms(new ModuloOnSuffix(1), true, endpoints)
                .setDataset(dataset);

        op = transforms.transform(op);
        assertTrue(op instanceof OpSequence);
        OpSequence opS = (OpSequence) op;
        assertEquals(2, opS.size());
        assertTrue(opS.get(0) instanceof OpTable);
        assertTrue(((OpTable)opS.get(0)).getTable().getVars().contains(Var.alloc("g1")));
        log.debug(op.toString());
    }

    @Disabled("Not up-to-date with To Source Selection Transform.")
    @Test
    public void optionals_without_cartesian_product_but_one_constant () {
        String queryAsString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?o
                    OPTIONAL { ?o <http://auth/nbPets> ?n }
                }
                """;

        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);

        ToSourceSelectionTransforms transforms = new ToSourceSelectionTransforms(new ModuloOnSuffix(1), true, endpoints)
                .setDataset(dataset);
        op = transforms.transform(op);
        OpLeftJoin lj = (OpLeftJoin) op;
        OpSequence os1 = (OpSequence) lj.getLeft();
        assertEquals(2, os1.size()); // table + bgp#1
        assertTrue(lj.getRight() instanceof OpQuad); // bgp2
        log.debug(op.toString());
    }

    @Disabled("Not up-to-date with to Source Selection transform.")
    @Test
    public void optionals_without_cartesian_product_but_with_constant_in_optional () {
        String queryAsString = """
                SELECT * WHERE {
                    <http://auth/David> <http://auth/owns> ?o
                    OPTIONAL { ?o <http://auth/family> <http://auth/canid> }
                }
                """;

        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);

        ToSourceSelectionTransforms toSS = new ToSourceSelectionTransforms(new ModuloOnSuffix(1), true, endpoints)
                .setDataset(dataset);
        op = toSS.transform(op);
        log.debug(op.toString());

        OpLeftJoin lj = (OpLeftJoin) op;
        OpSequence os1 = (OpSequence) lj.getLeft();
        assertEquals(2, os1.size()); // table + bgp#1
        // despite being a constant, bgp2 does not have a values since it has a shared variable
        assertTrue(lj.getRight() instanceof OpQuad);
        log.debug(op.toString());
    }

    @Disabled("Not up-to-date with ToSourceSelectionTransform.")
    @Test
    public void optionals_with_cartesian_product_and_constants () {
        String queryAsString = """
                SELECT * WHERE {
                    <http://auth/David> <http://auth/owns> ?o
                    OPTIONAL { ?x <http://auth/family> <http://auth/canid> }
                }
                """;

        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);

        ToSourceSelectionTransforms transforms = new ToSourceSelectionTransforms(new ModuloOnSuffix(1), true, endpoints)
                .setDataset(dataset);
        op = transforms.transform(op);
        OpLeftJoin lj = (OpLeftJoin) op;
        OpSequence os1 = (OpSequence) lj.getLeft();
        assertEquals(2, os1.size()); // table + bgp#1
        assertEquals(Var.alloc("g1"), ((OpTable) os1.get(0)).getTable().getVars().get(0));
        OpSequence os2 = (OpSequence) lj.getRight();
        // despite being a constant, bgp2 does not have a values since it has a shared variable
        assertEquals(2, os2.size()); // bgp2
        log.debug(op.toString());
    }

    @Disabled
    @Test
    public void a_union_with_constants () {
        // TODO TODO TODO
    }

    /* ************************************************************** */

    @Disabled
    @Test
    public void test_on_h0_batch_9_with_Q07f () {
        Dataset summary = TDB2Factory.connectDataset("./temp/fedup-id");
        summary.begin(ReadWrite.READ);
        log.debug("Number of quads in the dataset: " + summary.getUnionModel().size());
        summary.end();

        Set<String> endpoints = new HashSet<>();
        IntStream.range(0, 100).forEach( i -> {
                    endpoints.add(String.format("http://www.vendor%s.fr/", i));
                    endpoints.add(String.format("http://www.ratingsite%s.fr/", i)); });

        String queryAsString = """
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rev: <http://purl.org/stuff/rev#>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
                PREFIX dc: <http://purl.org/dc/elements/1.1/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                                
                SELECT DISTINCT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2 WHERE {
                    ?localProduct rdf:type bsbm:Product .
                    ?localProduct owl:sameAs bsbm:Product72080 .
                    ?localProduct rdfs:label ?productLabel .
                    OPTIONAL {
                        ?offer bsbm:product ?offerProduct .
                        ?offerProduct  owl:sameAs bsbm:Product72080 .
                        ?offer bsbm:price ?price .
                        ?offer bsbm:vendor ?vendor .
                        ?vendor rdfs:label ?vendorTitle .
                        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#FR> .
                        ?offer bsbm:validTo ?date .
                        FILTER (?date > "2008-04-25T00:00:00"^^xsd:dateTime )
                    }
                    OPTIONAL {
                        ?review bsbm:reviewFor ?reviewProduct .
                        ?reviewProduct owl:sameAs bsbm:Product72080 .
                        ?review rev:reviewer ?reviewer .
                        ?reviewer foaf:name ?revName .
                        ?review dc:title ?revTitle .
                        OPTIONAL { ?review bsbm:rating1 ?rating1 . }
                        OPTIONAL { ?review bsbm:rating2 ?rating2 . }
                    }
                }
                """;

        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);

        // Modulo suffix and summary is for random walks
        // ModuloOnSuffix mos = new ModuloOnSuffix(1);
        // op = Transformer.transform(mos, op);

        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(new ModuloOnSuffix(1), true, endpoints)
                .setDataset(dataset);
        op = tsst.transform(op);

        log.debug(op.toString());
        // the result is complex to describe,
        // so we just expect to get g1, g4, and g11 before their respective constant
    }

}