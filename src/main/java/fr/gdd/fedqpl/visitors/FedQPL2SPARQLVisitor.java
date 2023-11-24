package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.Iterator;

/**
 * Converts a FedQPL expression into a SPARQL {@link Op} service query. To get its
 * String version, please consider using `OpAsQuery.asQuery(op).toString()`.
 */
public class FedQPL2SPARQLVisitor extends ReturningOpVisitor<Op> {

    @Override
    public Op visit(OpService req) {
        return req;
    }

    @Override
    public Op visit(Mu mu) {
        return switch (mu.getElements().size()) {
            case 0 -> OpNull.create();
            case 1 -> ReturningOpVisitorRouter.visit(this, mu.getElements().iterator().next());
            default -> {
                // wrote as nested unions
                Iterator<Op> ops = mu.getElements().iterator();
                Op left = ReturningOpVisitorRouter.visit(this, ops.next());
                while (ops.hasNext()) {
                    Op right = ReturningOpVisitorRouter.visit(this, ops.next());
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

    @Override
    public Op visit(Mj mj) {
        return switch (mj.getElements().size()) {
            case 0 -> OpNull.create();
            case 1 -> ReturningOpVisitorRouter.visit(this, mj.getElements().iterator().next());
            default -> {
                // as nested joins
                Iterator<Op> ops = mj.getElements().iterator();
                Op left = ReturningOpVisitorRouter.visit(this, ops.next());
                while (ops.hasNext()) {
                    Op right = ReturningOpVisitorRouter.visit(this, ops.next());
                    left = OpJoin.create(left, right);
                }
                yield left;
            }
        };
    }

    @Override
    public Op visit(OpConditional lj) {
        return new OpConditional(ReturningOpVisitorRouter.visit(this, lj.getLeft()),
                ReturningOpVisitorRouter.visit(this, lj.getRight()));
    }

    @Override
    public Op visit(OpFilter filter) {
        return OpCloningUtil.clone(filter, ReturningOpVisitorRouter.visit(this, filter.getSubOp()));
    }

    @Override
    public Op visit(OpSlice limit) {
        return OpCloningUtil.clone(limit, ReturningOpVisitorRouter.visit(this, limit.getSubOp()));
    }

    @Override
    public Op visit(OpOrder orderBy) {
        return OpCloningUtil.clone(orderBy, ReturningOpVisitorRouter.visit(this, orderBy.getSubOp()));
    }

    @Override
    public Op visit(OpProject project) {
        return OpCloningUtil.clone(project, ReturningOpVisitorRouter.visit(this, project.getSubOp()));
    }

    @Override
    public Op visit(OpDistinct distinct) {
        return OpCloningUtil.clone(distinct, ReturningOpVisitorRouter.visit(this, distinct.getSubOp()));
    }
}
