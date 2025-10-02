package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.List;

/**
 * The visitor must implement this interface. Added value compared
 * to the default visitor {@link org.apache.jena.sparql.algebra.OpVisitor}:
 * it returns a type AND it passes arguments downstream.
 *
 * Remember to use the {@link ReturningArgsOpVisitorRouter} to call downstream visitors.
 * @param <A> The type of the argument to be passed.
 */
public class ReturningArgsOpBaseVisitor<A> extends ReturningArgsOpVisitor<Op, A> {
    public Op visit(Mu mu, A args) {
        List<Op> children = mu.getElements()
                .stream()
                .map((e) -> ReturningArgsOpVisitorRouter.visit(this, e, args))
                .toList();

        return new Mu(children);
    }

    public Op visit(Mj mj, A args) {
        List<Op> children = mj.getElements()
            .stream()
            .map((e) -> ReturningArgsOpVisitorRouter.visit(this, e, args))
            .toList();

        return new Mj(children);
    }

    public Op visit(OpService req, A args) {
        return new OpService(req.getService(), ReturningArgsOpVisitorRouter.visit(this, req.getSubOp(), args), req.getSilent());
    }

    public Op visit(OpTriple triple, A args) {
        return triple;
    }

    public Op visit(OpQuad quad, A args) {return quad;}
    public Op visit(OpBGP bgp, A args) {
        return bgp;
    }

    public Op visit(OpSequence sequence, A args) {
        OpSequence opSequence = OpSequence.create();
        sequence.getElements()
                .stream()
                .map((e) -> ReturningArgsOpVisitorRouter.visit(this, e, args))
                .forEach(opSequence::add);

        return opSequence;
    }
    public Op visit(OpTable table, A args) {return table.copy();}
    public Op visit(OpLeftJoin lj, A args) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, lj.getLeft(), args);
        Op right = ReturningArgsOpVisitorRouter.visit(this, lj.getRight(), args);

        return OpCloningUtil.clone(lj, left, right);
    }
    public Op visit(OpConditional cond, A args) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, cond.getLeft(), args);
        Op right = ReturningArgsOpVisitorRouter.visit(this, cond.getRight(), args);

        return OpCloningUtil.clone(cond, left, right);
    }
    public Op visit(OpFilter filter, A args) {
        return OpFilter.filterBy(filter.getExprs(), ReturningArgsOpVisitorRouter.visit(this, filter.getSubOp(), args));
    }
    public Op visit(OpUnion union, A args) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, union.getLeft(), args);
        Op right = ReturningArgsOpVisitorRouter.visit(this, union.getRight(), args);

        return OpCloningUtil.clone(union, left, right);
    }
    public Op visit(OpJoin join, A args) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), args);
        Op right = ReturningArgsOpVisitorRouter.visit(this, join.getRight(), args);

        return OpCloningUtil.clone(join, left, right);
    }

    public Op visit(OpDistinct distinct, A args) {
        return OpCloningUtil.clone(distinct, ReturningArgsOpVisitorRouter.visit(this, distinct.getSubOp(), args));
    }
    public Op visit(OpSlice slice, A args) {
        return OpCloningUtil.clone(slice, ReturningArgsOpVisitorRouter.visit(this, slice.getSubOp(), args));
    }
    public Op visit(OpOrder orderBy, A args)  {
        return OpCloningUtil.clone(orderBy, ReturningArgsOpVisitorRouter.visit(this, orderBy.getSubOp(), args));
    }
    public Op visit(OpProject project, A args) {
        return OpCloningUtil.clone(project, ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), args));
    }
    public Op visit(OpGroup groupBy, A args) {
        return OpCloningUtil.clone(groupBy, ReturningArgsOpVisitorRouter.visit(this, groupBy.getSubOp(), args));
    }
}
