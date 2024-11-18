package fr.gdd.fedup.asks;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ASKParallelTest {

    @Test
    public void testing_asks_on_one_graph_as_endpoint() {
        Dataset dataset = new InMemorySummaryFactory().getPetsDataset();

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
        Dataset dataset = new InMemorySummaryFactory().getPetsDataset();

        String graphA = "https://graphA.org";
        String graphB = "https://graphB.org";
        ASKParallel pa = new ASKParallel(Set.of(graphA, graphB));
        pa.setDataset(dataset);

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

    @Test
    public void test_on_remote_endpoints() {
        InMemorySummaryFactory imsf = new InMemorySummaryFactory();
        Node s = Var.alloc("s");
        Node p = NodeFactory.createURI("http://auth/named");
        Node o = NodeFactory.createURI("http://auth/Alice");
        Triple triple = Triple.create(s, p, o);

        // create the server
        FusekiServer serverA = FusekiServer.create()
                .port(3333)
                .add("graphA", imsf.getGraph("https://graphA.org"))
                .build();

        FusekiServer serverB = FusekiServer.create()
                .port(3334)
                .add("graphB", imsf.getGraph("https://graphB.org"))
                .build();

        String endpointA = "http://localhost:3333/graphA/sparql";
        String endpointB = "http://localhost:3334/graphB/sparql";

        ASKParallel pa = new ASKParallel(Set.of(endpointA));
        pa.execute(List.of(triple));
        assertFalse(pa.get(endpointA, triple)); // server does not run

        serverA.start();

        pa = new ASKParallel(Set.of(endpointA));
        pa.execute(List.of(triple));
        assertTrue(pa.get(endpointA, triple)); // server runs now

        serverB.start();
        pa = new ASKParallel(Set.of(endpointA, endpointB));
        pa.execute(List.of(triple));
        assertTrue(pa.get(endpointA, triple)); // still works for endpointA
        assertFalse(pa.get(endpointB, triple)); // but the endpointB does not have such triple

        serverA.stop();
        serverB.stop();
    }

}