package fr.gdd.fedup;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.FedQPL2SPARQLVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SA2FedQPLTest {

    Logger log = LoggerFactory.getLogger(SA2FedQPLTest.class);

    static String SILENT = "SILENT";

    @Test
    public void one_triple_and_its_unique_source_assignment () {
        String queryAsString = "SELECT * WHERE {?s ?p ?o}";
        Op op = Algebra.compile(QueryFactory.create(queryAsString));

        List<Map< Var, String>> assignments = List.of(
                Map.of(Var.alloc("g1"), "http://graphA")
        );

        ToQuadsTransform tqt = new ToQuadsTransform();
        tqt.transform(new OpBGP(BasicPattern.wrap(List.of(
                        new Triple(Var.alloc("s"), Var.alloc("p"), Var.alloc("o"))))));

        Op fedqpl = SA2FedQPL.build(op, assignments, tqt);
        assertTrue(fedqpl instanceof Mu);
        assertTrue(((Mu)fedqpl).getElements().size() == 1);
        assertTrue(((Mu)fedqpl).getElements().stream().allMatch(o->
            o instanceof Mj && ((Mj)o).getElements().size() == 1
        ));

        FedQPL2SPARQLVisitor toSPARQLVisitor = new FedQPL2SPARQLVisitor();
        Op asSPARQL = ReturningOpVisitorRouter.visit(toSPARQLVisitor, fedqpl);

        log.debug(OpAsQuery.asQuery(asSPARQL).toString());
        assertEquals(String.format("SELECT*WHERE{SERVICE%s<http://graphA>{?s?p?o}}", SILENT),
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
        tqt.transform(new OpBGP(BasicPattern.wrap(List.of(
                        new Triple(Var.alloc("s"), Var.alloc("p"), Var.alloc("o"))))));

        Op fedqpl = SA2FedQPL.build(op, assignments, tqt);
        assertTrue(fedqpl instanceof Mu);
        assertTrue(((Mu)fedqpl).getElements().size() == 2);
        assertTrue(((Mu)fedqpl).getElements().stream().allMatch(o->
                o instanceof Mj && ((Mj)o).getElements().size() == 1
        ));

        FedQPL2SPARQLVisitor toSparql = new FedQPL2SPARQLVisitor();
        op = ReturningOpVisitorRouter.visit(toSparql, fedqpl);
        assertEquals(String.format("SELECT*WHERE{{SERVICE%s<http://graphA>{?s?p?o}}UNION{SERVICE%s<http://graphB>{?s?p?o}}}", SILENT, SILENT),
                OpAsQuery.asQuery(op).toString()
                        .replace(" ", "")
                        .replace("\n", ""));
    }

}