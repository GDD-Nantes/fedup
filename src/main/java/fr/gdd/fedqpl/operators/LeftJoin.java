package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

/**
 * Left join operator.
 */
public class LeftJoin implements FedQPLOperator {

    FedQPLOperator left; // mandatory part
    FedQPLOperator right; // optional part

    public LeftJoin(FedQPLOperator left, FedQPLOperator right) {
        this.left = left;
        this.right = right;
    }

    public FedQPLOperator getLeft() {
        return left;
    }

    public FedQPLOperator getRight() {
        return right;
    }

    @Override
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
