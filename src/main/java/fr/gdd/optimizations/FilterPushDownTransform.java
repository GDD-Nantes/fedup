package fr.gdd.optimizations;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.optimize.VariableUsagePusher;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.core.Var;

import java.util.Set;

/**
 * Push the filter as soon as its variables are set.
 */
public class FilterPushDownTransform extends TransformCopy {

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        return pushDown(subOp, opFilter);
    }


    public Op pushDown(Op visited, OpFilter filter) {
        switch (visited) {
            case OpQuad o -> {
                return OpFilter.filterBy(filter.getExprs(), o);
            }
            case OpJoin o -> {
                if (tooFar(o.getLeft(), filter)) {
                    Op left = OpFilter.filterBy(filter.getExprs(), o.getLeft());
                    return OpJoin.create(left, o.getRight());
                } else {
                    return OpJoin.create(pushDown(o.getLeft(), filter), o.getRight());
                }
            }
            default -> throw new UnsupportedOperationException();
        }
    }

    public boolean tooFar(Op visited, OpFilter filter) {
        Set<Var> vars = filter.getExprs().getVarsMentioned();
        VariableUsageTracker vut = new VariableUsageTracker();
        VariableUsagePusher vuv = new VariableUsagePusher(vut);
        visited.visit(vuv);
        return vars.stream().noneMatch(v -> vut.getUsageCount(v) == 0);
    }


}
