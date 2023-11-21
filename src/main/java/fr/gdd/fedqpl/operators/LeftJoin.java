package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.Objects;

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

    @Override
    public <T,S> T visit(FedQPLVisitorArg<T,S> visitor, S arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LeftJoin leftJoin = (LeftJoin) o;
        return Objects.equals(left, leftJoin.left) && Objects.equals(right, leftJoin.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }
}
