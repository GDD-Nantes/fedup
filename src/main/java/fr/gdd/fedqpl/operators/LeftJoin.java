package fr.gdd.fedqpl.operators;

/**
 * Left join operator.
 */
public class LeftJoin {

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
}
