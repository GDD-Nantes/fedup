package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.expr.ExprList;

import java.util.Objects;

public class Filter implements FedQPLOperator {
    
    private ExprList expressions;
    private FedQPLOperator subOp;

    public Filter(ExprList expressions, FedQPLOperator op) {
        this.expressions = expressions;
        this.subOp = op;
    }

    public ExprList getExprs() {
        return this.expressions;
    }

    public FedQPLOperator getSubOp() {
        return this.subOp;
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
        Filter filter = (Filter) o;
        return Objects.equals(expressions, filter.expressions) && Objects.equals(subOp, filter.subOp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions, subOp);
    }
}
