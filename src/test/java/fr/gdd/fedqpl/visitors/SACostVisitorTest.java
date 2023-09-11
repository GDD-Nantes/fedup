package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Req;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SACostVisitorTest {

    @Test
    public void process_the_sa_cost_of_a_simple_plan() {
        Mj mj = new Mj();
        mj.addChild(new Req(new OpBGP(), NodeFactory.createURI("a")));
        mj.addChild(new Req(new OpBGP(), NodeFactory.createURI("b")));
        mj.addChild(new Req(new OpBGP(), NodeFactory.createURI("c")));
        SACostVisitor visitor = new SACostVisitor();
        Integer sacost = visitor.visit(mj);
        assertEquals(3, sacost);
    }

}