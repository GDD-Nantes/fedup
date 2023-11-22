package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.OpOrder;

import java.util.List;

public class OrderBy extends OpOrder implements FedQPLOperator {

    FedQPLOperator child;

    public OrderBy(List<SortCondition> orderBy) {
        super(null, orderBy);
    }

    public FedQPLOperator getChild() {
        return child;
    }

    public OrderBy setChild(FedQPLOperator child) {
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
