package fr.gdd.fedup.source.selection.transforms;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ToSourceSelectionTransformsTest {

    @Test
    public void adds_values_and_graph_closes() {
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

        ToSourceSelectionTransforms transforms = new ToSourceSelectionTransforms(endpoints, dataset);

        op = transforms.transform(op);
        assertTrue(op instanceof OpSequence);
        OpSequence opS = (OpSequence) op;
        assertEquals(2, opS.size());
        assertTrue(opS.get(0) instanceof OpTable);
        assertTrue(((OpTable)opS.get(0)).getTable().getVars().contains(Var.alloc("g1")));
    }

    @Test
    public void optionals_without_cartesian_product () {
        // TODO TODO TODO TODO
    }

}