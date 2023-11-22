package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import org.apache.jena.sparql.algebra.op.OpSlice;

import java.util.List;

/**
 * Even though not defined in FedQPL Algebra, this query modifier
 * must be taken into account and can lead to huge performance gain/loss
 * when positioned right.
 */
public class Limit extends OpSlice implements FedQPLOperator {

    FedQPLOperator child;

    public Limit(Long start, Long limit) { // TODO offset
        super(null, start, limit);
    }

    public FedQPLOperator getChild() {
        return child;
    }

    public Limit setChild(FedQPLOperator child) {
        this.child = child;
        return this;
    }

    @Override
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <T,S> T visit(FedQPLVisitorArg<T,S> visitor, S arg) {
        return visitor.visit(this, arg);
    }
}
