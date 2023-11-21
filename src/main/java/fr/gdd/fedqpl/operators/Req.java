package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
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
import java.util.Objects;
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

    @Override
    public <T,S> T visit(FedQPLVisitorArg<T,S> visitor, S arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Req req = (Req) o;
        return Objects.equals(op, req.op) && Objects.equals(source, req.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, source);
    }
}
