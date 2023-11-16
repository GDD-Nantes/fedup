package fr.gdd.fedup.transforms;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ToValuesTransformTest {

    @Test
    public void reordering_a_simple_bgp_with_a_values() {
        Dataset dataset = InMemorySummaryFactory.getPetsDataset();

        String graphA = "https://graphA.org";
        String graphB = "https://graphB.org";
        Set<String> endpoints = Set.of(graphB, graphA);

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

        ToValuesAndOrderTransform vt = new ToValuesAndOrderTransform(endpoints);
        vt.setDataset(dataset);
        op = vt.transform(op);

        Triple triple = Triple.create(Var.alloc("s"),
                NodeFactory.createURI("http://auth/named"),
                NodeFactory.createURI("http://auth/Alice"));

        assertTrue(op instanceof OpSequence);
        assertTrue(((OpSequence)op).get(0) instanceof OpTable);
        assertEquals(1, ((OpTable)((OpSequence)op).get(0)).getTable().size() ); // only graphA
        assertTrue(((OpSequence)op).get(1) instanceof OpBGP);
        assertTrue(((OpBGP)((OpSequence)op).get(1)).getPattern().get(0).matches(triple));
    }

}