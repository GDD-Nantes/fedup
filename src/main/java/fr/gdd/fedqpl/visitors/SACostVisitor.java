package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;

/**
 * Processes the SACost of the visited plan, i.e., the number of Request `Req` clauses
 * of the plan.
 */
public class SACostVisitor extends FedQPLVisitor<Integer, Object> {

    public Integer visit(FedQPLOperator op) {
        return (Integer) op.visit(this, null);
    }

    @Override
    public Integer visit(Mj mj, Object args) {
        Integer sum = 0;
        for (FedQPLOperator op : mj.getChildren()) {
            sum += (Integer) op.visit(this, null);
        }
        return sum;
    }

    @Override
    public Integer visit(Mu mu, Object args) {
        Integer sum = 0;
        for (FedQPLOperator op : mu.getChildren()) {
            sum += (Integer) op.visit(this, null);
        }
        return sum;
    }

    @Override
    public Integer visit(LeftJoin lj, Object args) {
        return (Integer) lj.getRight().visit(this, null) +
                (Integer) lj.getLeft().visit(this, null);
    }

    @Override
    public Integer visit(Req req, Object args) {
        return 1;
    }
}
