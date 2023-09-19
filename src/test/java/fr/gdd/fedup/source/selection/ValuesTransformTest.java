package fr.gdd.fedup.source.selection;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ValuesTransformTest {

    @Test
    public void reordering_a_simple_bgp_with_a_values() {
        Dataset dataset = InMemorySummaryFactory.getPetsDataset();

        String graphA = "https://graphA.org";
        String graphB = "https://graphB.org";

        // stupid query where the second triple pattern should be the first
        // one, since Alice is a constant.
        // In addition, we expect a values with only one graph since Alice exists
        // only at `graphA`.
        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/named> ?o
                    ?s <http://auth/named> <http://auth/Alice>
                }
                """;
        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);

    }

}