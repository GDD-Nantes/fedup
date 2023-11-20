package fr.gdd.fedup;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.FedQPL2SPARQLVisitor;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SA2FedQPLTest {

    Logger log = LoggerFactory.getLogger(SA2FedQPLTest.class);

    @Test
    public void one_triple_and_its_unique_source_assignment () {
        String queryAsString = "SELECT * WHERE {?s ?p ?o}";
        Op op = Algebra.compile(QueryFactory.create(queryAsString));

        List<Map< Var, String>> assignments = List.of(
                Map.of(Var.alloc("g1"), "http://graphA")
        );

        ToQuadsTransform tqt = new ToQuadsTransform();
        tqt.add(Var.alloc("g1"), new Quad(Var.alloc("g1"), Var.alloc("s"), Var.alloc("p"), Var.alloc("o")));

        FedQPLOperator fedqpl = SA2FedQPL.build(op, assignments, tqt);
        assertTrue(fedqpl instanceof Mu);
        assertTrue(((Mu)fedqpl).getChildren().size() == 1);
        assertTrue(((Mu)fedqpl).getChildren().stream().allMatch(o->
            o instanceof Mj && ((Mj)o).getChildren().size() == 1
        ));
        /* assertEquals(new Mu(Set.of(new Mj(Set.of(
                new Req(
                        new Triple(Var.alloc("s"), Var.alloc("p"), Var.alloc("o")),
                        NodeFactory.createURI("http://graphA")
                ))))), fedqpl.toString());*/

        FedQPL2SPARQLVisitor toSPARQLVisitor = new FedQPL2SPARQLVisitor();
        Op asSPARQL = fedqpl.visit(toSPARQLVisitor);

        log.debug(OpAsQuery.asQuery(asSPARQL).toString());
        assertEquals("SELECT*WHERE{SERVICE<http://graphA>{?s?p?o}}",
                OpAsQuery.asQuery(asSPARQL).toString().replace(" ", "")
                        .replace("\n", "")
        );
    }

    @Test
    public void one_triple_and_two_source_assignments () {
        String queryAsString = "SELECT * WHERE {?s ?p ?o}";
        Op op = Algebra.compile(QueryFactory.create(queryAsString));

        List<Map< Var, String>> assignments = List.of(
                Map.of(Var.alloc("g1"), "http://graphA"),
                Map.of(Var.alloc("g1"), "http://graphB")
        );

        ToQuadsTransform tqt = new ToQuadsTransform();
        tqt.add(Var.alloc("g1"), new Quad(Var.alloc("g1"), Var.alloc("s"), Var.alloc("p"), Var.alloc("o")));

        FedQPLOperator fedqpl = SA2FedQPL.build(op, assignments, tqt);
        assertTrue(fedqpl instanceof Mu);
        assertTrue(((Mu)fedqpl).getChildren().size() == 2);
        assertTrue(((Mu)fedqpl).getChildren().stream().allMatch(o->
                o instanceof Mj && ((Mj)o).getChildren().size() == 1
        ));

        FedQPL2SPARQLVisitor toSparql = new FedQPL2SPARQLVisitor();
        op = fedqpl.visit(toSparql);
        assertEquals("SELECT*WHERE{{SERVICE<http://graphA>{?s?p?o}}UNION{SERVICE<http://graphB>{?s?p?o}}}",
                OpAsQuery.asQuery(op).toString()
                        .replace(" ", "")
                        .replace("\n", ""));
    }

}