package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Apply basic rewrittings to simplify the FedQPL expression.
 */
public class FedQPLSimplifyVisitor implements FedQPLVisitor<FedQPLOperator> {

    @Override
    public FedQPLOperator visit(Req req) {
        return req; //nothing
    }

    @Override
    public FedQPLOperator visit(Mj mj) {
        List<FedQPLOperator> children = mj.getChildren().stream().map(c -> c.visit(this)).collect(Collectors.toList());
        if (children.isEmpty()) {
            return null;
        } else if (children.size() == 1) {
            return children.getFirst();
        } else {
            return new Mj(children);
        }
    }

    @Override
    public FedQPLOperator visit(Mu mu) {
        List<FedQPLOperator> children = mu.getChildren().stream().map(c -> c.visit(this)).collect(Collectors.toList());
        if (children.isEmpty()) {
            return null;
        } else if (children.size() == 1) {
            return children.getFirst();
        } else {
            return new Mu(children);
        }
    }

    @Override
    public FedQPLOperator visit(LeftJoin lj) {
        FedQPLOperator left = lj.getLeft().visit(this);
        FedQPLOperator right = lj.getRight().visit(this);

        if (Objects.isNull(left)) {
            return null;
        }
        if (Objects.isNull(right)) {
            return left;
        }
        return new LeftJoin(left, right);
    }
}
