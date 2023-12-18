package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.visitors.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.main.VarFinder;

import java.util.HashSet;
import java.util.Set;

/**
 * Pushing filter as close as possible of where their binding is
 * generated is beneficial. Most of all, we do not want to keep
 * them on the federation engine, as they may reduce traffic.
 */
public class FilterPushDownVisitor extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpFilter filter) {
        return ReturningArgsOpVisitorRouter.visit(
                new FilterPushDownVisitorStarted(),
                ReturningOpVisitorRouter.visit(this, filter.getSubOp()), // visit downstream first
                filter);
    }

    /* ********************************************************************* */

    /**
     * The filter is fixed, now it must be put at the right place. We only explore things that we added for federated querying.
     */
    public static class FilterPushDownVisitorStarted extends ReturningArgsOpVisitor<Op, OpFilter> {

        @Override
        public Op visit(OpService req, OpFilter args) {
            // Even when a variable is not defined, it should be pushed,
            // because it triggers an Error, and Error is defined to be falsy.
            Op inside = Transformer.transform(new TransformFilterPlacement(),
                    OpCloningUtil.clone(args, req.getSubOp()));

            return OpCloningUtil.clone(req, inside);
        }

        @Override
        public Op visit(OpFilter filter, OpFilter args) {
            return OpCloningUtil.clone(filter, ReturningArgsOpVisitorRouter.visit(this, filter.getSubOp(), args));
        }

        @Override
        public Op visit(OpUnion union, OpFilter args) {
            return OpCloningUtil.clone(union,
                    ReturningArgsOpVisitorRouter.visit(this, union.getLeft(), args),
                    ReturningArgsOpVisitorRouter.visit(this, union.getRight(), args));
        }

        @Override
        public Op visit(OpJoin join, OpFilter args) {
            Set<Var> filterMentioned = args.getExprs().getVarsMentioned();
            VarFinder varsLeft = VarFinder.process(join.getLeft());
            Set<Var> leftMentioned = new HashSet<>(varsLeft.getAssign());
            leftMentioned.addAll(varsLeft.getOpt());
            leftMentioned.addAll(varsLeft.getFixed());


            if (leftMentioned.containsAll(filterMentioned)) {
                return OpCloningUtil.clone(join,
                        ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), args),
                        join.getRight());
            } else {
                VarFinder varsRight = VarFinder.process(join.getRight());
                Set<Var> rightMentioned = new HashSet<>(varsRight.getAssign());
                rightMentioned.addAll(varsRight.getOpt());
                rightMentioned.addAll(varsRight.getFixed());
                // TODO depending on operator, allows filter to go down further, but conservative is
                // TODO to stay on top.
                // rightMentioned.addAll(leftMentioned); // left are already set (/!\ no symmetric hash join plz)
                if (rightMentioned.containsAll(filterMentioned)) {
                    return OpCloningUtil.clone(join,
                            join.getLeft(),
                            ReturningArgsOpVisitorRouter.visit(this, join.getRight(), args));
                } else {
                    return OpCloningUtil.clone(args, join); // filter on top of join
                }
            }
        }

        @Override
        public Op visit(OpSequence sequence, OpFilter args) {
            // TODO double check that it is actually one of our added VALUES
            // TODO that simplifies the query expression.
            // TODO or simply act as normal, but must think about this as cascading joins.
            return OpCloningUtil.clone(sequence,
                    sequence.getElements().stream().map(c-> ReturningArgsOpVisitorRouter.visit(this, c, args)).toList()
            );
        }

        @Override
        public Op visit(OpTable table, OpFilter args) {
            return table;
        }
    }
}
