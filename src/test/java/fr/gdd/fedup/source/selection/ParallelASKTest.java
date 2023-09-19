package fr.gdd.fedup.source.selection;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionDatasetBuilder;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParallelASKTest {

    @Test
    public void testing_asks_on_one_graph_as_endpoint() {
        Dataset dataset = InMemorySummaryFactory.getPetsDataset();

        String graphA = "https://graphA.org";
        String graphB = "https://graphB.org";
        ASKParallel pa = new ASKParallel(Set.of(graphA));
        pa.setDataset(dataset);

        Triple triple = Triple.create(Var.alloc("s"),
                NodeFactory.createURI("http://auth/named"),
                NodeFactory.createURI("http://auth/Alice"));

        pa.execute(List.of(triple));

        assertTrue(pa.get(graphA, triple));
        assertFalse(pa.get(graphB, triple));
    }

    @Test
    public void testing_asks_on_two_local_endpoints() {
        Dataset dataset = InMemorySummaryFactory.getPetsDataset();

        String graphA = "https://graphA.org";
        String graphB = "https://graphB.org";
        ASKParallel pa = new ASKParallel(Set.of(graphA, graphB));
        pa.setDataset(dataset);

        QueryExecutionDatasetBuilder qedb = new QueryExecutionDatasetBuilder();
        qedb.dataset(dataset);
        pa.builder = qedb;

        Node s = Var.alloc("s");
        Node p = NodeFactory.createURI("http://auth/named");
        Node o = NodeFactory.createURI("http://auth/Alice");

        Triple triple = Triple.create(s, p, o);
        pa.execute(List.of(triple));

        assertTrue(pa.get(graphA, triple));
        assertFalse(pa.get(graphB, triple));

        Node s2 = Var.alloc("s");
        Node p2 = NodeFactory.createURI("http://auth/named");
        Node o2 = NodeFactory.createURI("http://auth/Carol");
        Triple triple2 = Triple.create(s2, p2, o2);

        pa.execute(List.of(triple2));
        assertTrue(pa.get(graphA, triple));
        assertFalse(pa.get(graphB, triple));
        assertFalse(pa.get(graphA, triple2));
        assertTrue(pa.get(graphB, triple2));
    }

    @Disabled
    @Test
    public void test_on_remote_datasets() {

    }

}