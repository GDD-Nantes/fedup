package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.visitors.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.optimize.VariableUsagePusher;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.core.Var;

import java.util.Objects;
import java.util.Set;

/**
 * Pushing filter as close as possible of where their binding is
 * generated is beneficial. Most of all, we do not want to keep
 * them on the federation engine, as they may reduce traffic.
 * TODO
 */
public class FilterPushDownVisitor extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpFilter filter) {
        return ReturningArgsOpVisitorRouter.visit(
                new FilterPushDownVisitorStarted(),
                ReturningOpVisitorRouter.visit(this, filter.getSubOp()), // visit downstream first
                filter);
    }

    /**
     * The filter is fixed, now it must be put at the right place.
     */
    public static class FilterPushDownVisitorStarted extends ReturningArgsOpVisitor<Op, OpFilter> {

        @Override
        public Op visit(OpFilter filter, OpFilter args) {
            return OpCloningUtil.clone(filter, ReturningArgsOpVisitorRouter.visit(this, filter.getSubOp(), args));
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
}
