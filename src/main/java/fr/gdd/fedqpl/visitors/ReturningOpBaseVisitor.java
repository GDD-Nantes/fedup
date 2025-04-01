package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.List;

/**
 * A visitor dedicated to returning `Op`.
 */
public class ReturningOpBaseVisitor extends ReturningOpVisitor<Op> {

    @Override
    public Op visit(Mu mu) {
        return new Mu(this.visit(mu.getElements()));
    }

    @Override
    public Op visit(Mj mj) {
        return new Mj(this.visit(mj.getElements()));
    }

    @Override
    public Op visit(OpService req) {
        return req;
    }

    @Override
    public Op visit(OpTriple triple) {
        return triple;
    }

    @Override
    public Op visit(OpQuad quad) {
        return quad;
    }

    @Override
    public Op visit(OpBGP bgp) {
        return bgp;
    }

    @Override
    public Op visit(OpGraph graph) {
        return OpCloningUtil.clone(graph, ReturningOpVisitorRouter.visit(this, graph.getSubOp()));
    }

    @Override
    public Op visit(OpSequence sequence) {
        OpSequence newSequence = OpSequence.create();
        for (Op subop : sequence.getElements()) {
            newSequence.add(ReturningOpVisitorRouter.visit(this, subop));
        }
        return newSequence;
    }

    @Override
    public Op visit(OpTable table) {
        return table;
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        return OpCloningUtil.clone(lj, ReturningOpVisitorRouter.visit(this, lj.getLeft()),
                ReturningOpVisitorRouter.visit(this, lj.getRight()));
    }

    @Override
    public Op visit(OpConditional cond) {
        return OpCloningUtil.clone(cond, ReturningOpVisitorRouter.visit(this, cond.getLeft()),
                ReturningOpVisitorRouter.visit(this, cond.getRight()));
    }

    @Override
    public Op visit(OpFilter filter) {
        return OpCloningUtil.clone(filter, ReturningOpVisitorRouter.visit(this, filter.getSubOp()));
    }

    @Override
    public Op visit(OpUnion union) {
       return new OpUnion(ReturningOpVisitorRouter.visit(this, union.getLeft()), ReturningOpVisitorRouter.visit(this, union.getRight()));
    }

    @Override
    public Op visit(OpJoin join) {
        return OpJoin.create(ReturningOpVisitorRouter.visit(this, join.getLeft()), ReturningOpVisitorRouter.visit(this, join.getRight()));
    }

    @Override
    public Op visit(OpDistinct distinct) {
        return OpCloningUtil.clone(distinct, ReturningOpVisitorRouter.visit(this, distinct.getSubOp()));
    }

    @Override
    public Op visit(OpSlice slice) {
        return OpCloningUtil.clone(slice, ReturningOpVisitorRouter.visit(this, slice.getSubOp()));
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
    public Op visit(OpGroup groupBy) {
        return OpCloningUtil.clone(groupBy, ReturningOpVisitorRouter.visit(this, groupBy.getSubOp()));
    }

    /**
     * Visit all children and apply the visitor.
     * @param children The children to visit.
     * @return List of `Op` resulting from the visit.
     */
    public List<Op> visit(List<Op> children) {
        return children.stream().map(c -> ReturningOpVisitorRouter.visit(this, c)).toList();
    }

    @Override
    public Op visit(OpExtend extend) {
        return OpCloningUtil.clone(extend, ReturningOpVisitorRouter.visit(this, extend.getSubOp()));
    }
}
