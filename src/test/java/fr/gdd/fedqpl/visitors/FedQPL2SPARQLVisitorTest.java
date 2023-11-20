package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FedQPL2SPARQLVisitorTest {

    Logger log = LoggerFactory.getLogger(FedQPL2SPARQLVisitorTest.class);

    @Test
    public void simple_req_writes_as_a_service() {
        Req r = new Req(new OpTriple(new Triple(Var.alloc("s"),
                Var.alloc("p"),
                Var.alloc("o"))), NodeFactory.createURI("http://graphA"));

        FedQPL2SPARQLVisitor toSparql = new FedQPL2SPARQLVisitor();
        Op op = toSparql.visit(r);

        assertEquals("SELECT*WHERE{SERVICE<http://graphA>{?s?p?o}}",
                OpAsQuery.asQuery(op).toString().replace(" ", "")
                        .replace("\n", "")
                );
    }

    @Test
    public void req_with_two_triples_in_it() {
        Req r = new Req(new OpBGP(BasicPattern.wrap(List.of(new Triple(Var.alloc("s"),
                Var.alloc("p"),
                Var.alloc("o")),
                new Triple(Var.alloc("s2"),
                        Var.alloc("p2"),
                        Var.alloc("o2"))))),
                NodeFactory.createURI("http://graphA"));

        FedQPL2SPARQLVisitor toSparql = new FedQPL2SPARQLVisitor();
        Op op = toSparql.visit(r);

        assertEquals("SELECT*WHERE{SERVICE<http://graphA>{?s?p?o.?s2?p2?o2}}",
                OpAsQuery.asQuery(op).toString().replace(" ", "")
                        .replace("\n", "")
        );
    }

    @Test
    public void simple_union_of_two_req() {
        Req r1 = new Req(new OpTriple(new Triple(Var.alloc("s"),
                Var.alloc("p"),
                Var.alloc("o"))),
                NodeFactory.createURI("http://graphA"));
        Req r2 = new Req(new OpTriple(new Triple(Var.alloc("s"),
                Var.alloc("p"),
                Var.alloc("o"))),
                NodeFactory.createURI("http://graphB"));
        Mu mu = new Mu(List.of(r1, r2));

        FedQPL2SPARQLVisitor toSparql = new FedQPL2SPARQLVisitor();
        Op op = toSparql.visit(mu);

        assertEquals("SELECT*WHERE{{SERVICE<http://graphA>{?s?p?o}}UNION{SERVICE<http://graphB>{?s?p?o}}}",
                OpAsQuery.asQuery(op).toString()
                        .replace(" ", "")
                        .replace("\n", ""));
    }

}