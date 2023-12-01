package fr.gdd.fedup.summary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.*;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HashSummaryTest {

    @Test
    public void simple_test_of_hash_summary_with_one_quad() {
        Summary hgs = SummaryFactory.createModuloOnSuffix(1);
        Node graphURI = NodeFactory.createURI("https://example.com/Graph1");
        hgs.add(Quad.create(graphURI,
                NodeFactory.createURI("https://example.com/Alice"),
                NodeFactory.createURI("https://example.com/hasFriend"),
                NodeFactory.createURI("https://example.com/Julien")));
        hgs.getSummary().begin(ReadWrite.READ);
        assertEquals(1, hgs.getSummary().getNamedModel(graphURI.getURI()).size());
        hgs.getSummary().end();
    }

    @Test
    public void summarize_actually_summarize() {
        Summary hgs = SummaryFactory.createModuloOnSuffix(1);
        Node graphURI = NodeFactory.createURI("https://example.com/Graph1");
        hgs.add(Quad.create(graphURI,
                NodeFactory.createURI("https://example.com/Alice"),
                NodeFactory.createURI("https://example.com/hasFriend"),
                NodeFactory.createURI("https://example.com/Julien")));
        hgs.add(Quad.create(graphURI,
                NodeFactory.createURI("https://example.com/Julien"),
                NodeFactory.createURI("https://example.com/hasFriend"),
                NodeFactory.createURI("https://example.com/Alice")));
        hgs.getSummary().begin(ReadWrite.READ);

        Query q = QueryFactory.create("SELECT * WHERE {GRAPH ?g {?s ?p ?o}}");
        QueryExecution qe = QueryExecutionFactory.create(q, hgs.getSummary());
        ResultSet rs = qe.execSelect();

        assertTrue(rs.hasNext());
        QuerySolution qs = rs.next();
        assertEquals(graphURI.toString(), qs.getResource("g").getURI());
        assertEquals("https://example.com/0", qs.getResource("s").getURI());
        assertEquals("https://example.com/hasFriend", qs.getResource("p").getURI());
        assertEquals("https://example.com/0", qs.getResource("o").getURI());
        assertFalse(rs.hasNext());

        assertEquals(1, hgs.getSummary().getNamedModel(graphURI.getURI()).size());
        hgs.getSummary().end();
    }

    /* ******************************************************************* */

    @Test
    public void summary_everything_ie_the_authority_too_and_constants () {
        Summary hgs = SummaryFactory.createModuloOnWhole(1);

        Node graphURI = NodeFactory.createURI("https://example.com/Graph1");
        hgs.add(Quad.create(graphURI,
                NodeFactory.createURI("https://example.com/Alice"),
                NodeFactory.createURI("https://example.com/hasFriend"),
                NodeFactory.createURI("https://example.com/Julien")));
        hgs.add(Quad.create(graphURI,
                NodeFactory.createURI("https://example.com/Julien"),
                NodeFactory.createURI("https://example.com/hasFriend"),
                NodeFactory.createURI("https://example.com/Alice")));
        hgs.getSummary().begin(ReadWrite.READ);

        Query q = QueryFactory.create("SELECT * WHERE {GRAPH ?g {?s ?p ?o}}");
        QueryExecution qe = QueryExecutionFactory.create(q, hgs.getSummary());
        ResultSet rs = qe.execSelect();

        assertTrue(rs.hasNext());
        QuerySolution qs = rs.next();
        assertEquals(graphURI.toString(), qs.getResource("g").getURI());
        assertEquals("https://0", qs.getResource("s").getURI());
        assertEquals("https://example.com/hasFriend", qs.getResource("p").getURI());
        assertEquals("https://0", qs.getResource("o").getURI());
        assertFalse(rs.hasNext());

        assertEquals(1, hgs.getSummary().getNamedModel(graphURI.getURI()).size());
        hgs.getSummary().end();
    }

}