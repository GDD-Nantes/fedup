package fr.gdd.optimizations;

import fr.gdd.fedup.transforms.ToSourceSelectionTransforms;
import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

class FilterPushDownTransformTest {

    Logger log = LoggerFactory.getLogger(FilterPushDownTransformTest.class);

    static Dataset dataset;
    static Set<String> endpoints = Set.of("https://graphA.org", "https://graphB.org");

    @BeforeAll
    public static void initialize_dataset() {
        dataset = InMemorySummaryFactory.getPetsDataset();
        dataset.begin(ReadWrite.READ);
    }

    @AfterAll
    public static void drop_dataset() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }


    @Disabled
    @Test
    public void filter_in_joined_quads() {
     String queryAsString = """
            SELECT * WHERE {
                ?p <http://auth/owns> ?a .
                ?a <http://auth/family> ?f
                FILTER (?a = "<http://auth/cat>")
            }
            """;

    Query query = QueryFactory.create(queryAsString);
    Op op = Algebra.compile(query);

    log.debug(op.toString());

    FilterPushDownTransform fpdt = new FilterPushDownTransform();
    op = Transformer.transform(fpdt, op);
    log.debug(op.toString());

    // TODO assert something
}

}