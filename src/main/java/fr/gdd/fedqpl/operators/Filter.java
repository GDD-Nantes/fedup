package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import org.apache.jena.sparql.expr.ExprList;

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
}
