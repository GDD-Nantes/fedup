package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PrinterVisitorTest {

    @Test
    public void simple_printer () {
        Mu mu = new Mu();
        Mj mj = new Mj();
        mj.addChild(new Req(new OpBGP(), NodeFactory.createURI("http://a")));
        mj.addChild(new Req(new OpBGP(), NodeFactory.createURI("http://b")));

        mu.addChild(mj);

        PrinterVisitor pv = new PrinterVisitor();
        pv.visit(mu);
    }

}