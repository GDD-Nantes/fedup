package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.visitors.*;
import org.apache.http.client.utils.CloneUtils;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.algebra.optimize.VariableUsagePusher;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
     * The filter is fixed, now it must be put at the right place.
     */
    public static class FilterPushDownVisitorStarted extends ReturningArgsOpVisitor<Op, OpFilter> {

        @Override
        public Op visit(OpService req, OpFilter args) {
            Op inside = Transformer.transform(new TransformFilterPlacement(),
                    OpCloningUtil.clone(args, req.getSubOp()));

            return OpCloningUtil.clone(req, inside);
        }

        @Override
        public Op visit(OpFilter filter, OpFilter args) {
            return OpCloningUtil.clone(filter, ReturningArgsOpVisitorRouter.visit(this, filter.getSubOp(), args));
        }

        @Override
        public Op visit(OpSequence sequence, OpFilter args) {
            return OpCloningUtil.clone(sequence,
                    sequence.getElements().stream().map(c-> ReturningArgsOpVisitorRouter.visit(this, c, args)).toList()
            );
        }

        @Override
        public Op visit(OpUnion union, OpFilter args) {
            return OpCloningUtil.clone(union,
                    ReturningArgsOpVisitorRouter.visit(this, union.getLeft(), args),
                    ReturningArgsOpVisitorRouter.visit(this, union.getRight(), args));
        }

        @Override
        public Op visit(OpTable table, OpFilter args) {
            return table;
        }
    }
}
