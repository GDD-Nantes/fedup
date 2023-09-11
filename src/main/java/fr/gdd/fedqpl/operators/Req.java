package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.op.OpBGP;

/**
 * Request.
 */
public class Req extends FedQPLOperator {

    OpBGP bgp;
    Node source;

    public Req(OpBGP bgp, Node source) {
        this.bgp = bgp;
        this.source = source;
    }

    public OpBGP getBgp() {
        return bgp;
    }

    public Node getSource() {
        return source;
    }

    public Object visit(FedQPLVisitor visitor, Object args) {
        return visitor.visit(this, args);
    }
}
