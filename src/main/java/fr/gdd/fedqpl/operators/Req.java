package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.List;
import java.util.Set;

/**
 * Request. The whole `Op` is meant to be executed on a
 * single remote endpoint.
 */
public class Req implements FedQPLOperator {

    Op op;
    Node source;

    public Req(Op op, Node source) {
        this.op = op;
        this.source = source;
    }

    public Op getOp() {
        return op;
    }

    public Node getSource() {
        return source;
    }

    @Override
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
