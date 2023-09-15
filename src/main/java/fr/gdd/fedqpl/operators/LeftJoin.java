package fr.gdd.fedqpl.operators;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

/**
 * Left join operator.
 */
public class LeftJoin extends FedQPLOperator {

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
    public void visit(OpVisitor opVisitor) {
        // TODO Auto-generated method stub
        if (!(opVisitor instanceof FedQPLVisitor)) {
            throw new IllegalArgumentException("The visitor should be an instance of FedQPLVisitor");
        }
        FedQPLVisitor visitor = (FedQPLVisitor) opVisitor;
        left.visit(visitor);
        right.visit(visitor);
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return "LeftJoin";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return left.hashCode() << 1 ^ right.hashCode() ^ getName().hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        // TODO Auto-generated method stub
        if (!(other instanceof LeftJoin))
            return false;
        LeftJoin opJoin = (LeftJoin) other;
        return (opJoin.left.equalTo(left, labelMap) && opJoin.right.equalTo(right, labelMap) );

    }
}
