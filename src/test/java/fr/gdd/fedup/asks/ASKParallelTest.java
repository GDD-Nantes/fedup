package fr.gdd.fedup.asks;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ASKParallelTest {

    @Test
    public void testing_asks_on_one_graph_as_endpoint() {
        final InMemorySummaryFactory pets = new InMemorySummaryFactory();
        final String graphA = "https://graphA.org";
        final String graphB = "https://graphB.org";
        final ASKParallel pa = new ASKParallel(Set.of(graphA));
        pa.setDataset(pets.getPetsDataset());

        final Triple triple = Triple.create(Var.alloc("s"),
                NodeFactory.createURI("http://auth/named"),
                NodeFactory.createURI("http://auth/Alice"));

        pa.execute(List.of(triple));

        assertTrue(pa.get(graphA, triple));
        assertFalse(pa.get(graphB, triple));
        pets.close();
    }

    @Test
    public void testing_asks_on_two_local_endpoints() {
        final InMemorySummaryFactory pets = new InMemorySummaryFactory();
        final String graphA = "https://graphA.org";
        final String graphB = "https://graphB.org";
        final ASKParallel pa = new ASKParallel(Set.of(graphA, graphB));
        pa.setDataset(pets.getPetsDataset());

        final Node s = Var.alloc("s");
        final Node p = NodeFactory.createURI("http://auth/named");
        final Node o = NodeFactory.createURI("http://auth/Alice");

        final Triple triple = Triple.create(s, p, o);
        pa.execute(List.of(triple));

        assertTrue(pa.get(graphA, triple));
        assertFalse(pa.get(graphB, triple));

        final Node s2 = Var.alloc("s");
        final Node p2 = NodeFactory.createURI("http://auth/named");
        final Node o2 = NodeFactory.createURI("http://auth/Carol");
        final Triple triple2 = Triple.create(s2, p2, o2);

        pa.execute(List.of(triple2));
        assertTrue(pa.get(graphA, triple));
        assertFalse(pa.get(graphB, triple));
        assertFalse(pa.get(graphA, triple2));
        assertTrue(pa.get(graphB, triple2));
        pets.close();
    }

    @Test
    public void test_on_remote_endpoints() {
        final InMemorySummaryFactory pets = new InMemorySummaryFactory();
        final Node s = Var.alloc("s");
        final Node p = NodeFactory.createURI("http://auth/named");
        final Node o = NodeFactory.createURI("http://auth/Alice");
        final Triple triple = Triple.create(s, p, o);

        // create the server
        FusekiServer serverA = FusekiServer.create()
                .port(3333)
                .add("graphA", pets.getGraph("https://graphA.org"))
                .build();
        FusekiServer serverB = FusekiServer.create()
                .port(3334)
                .add("graphB", pets.getGraph("https://graphB.org"))
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
        pets.close();
    }

}