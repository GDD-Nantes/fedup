package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.List;
import java.util.Map;

public class OrderMuByNumberOfReq extends ReturningOpBaseVisitor {

    @Override
    public Op visit(Mu mu) {
        List<Op> ordered = mu.getElements().stream().map(o -> {
                    Op subOp = ReturningOpVisitorRouter.visit(this, o);
                    return Pair.of(subOp, ReturningOpVisitorRouter.visit(new CountNumberOfServices(), subOp)); })
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .toList().reversed();

        return new Mu(ordered);
    }

    @Override
    public Op visit(Mj mj) {
        List<Op> ordered = mj.getElements().stream().map(o ->
                        Pair.of(o, ReturningOpVisitorRouter.visit(new CountNumberOfTriples(), o))
                ).sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .toList().reversed();

        return new Mj(ordered);
    }

    public class CountNumberOfServices extends ReturningOpVisitor<Integer> {

        @Override public Integer visit(OpService req) { return 1; }

        @Override public Integer visit(OpTriple triple) { return 0; }
        @Override public Integer visit(OpQuad quad) { return 0; }
        @Override public Integer visit(OpQuadBlock block) { return 0; }
        @Override public Integer visit(OpQuadPattern quads) { return 0; }
        @Override public Integer visit(OpBGP bgp) { return 0; }
        @Override public Integer visit(OpTable table) { return 0; }

        @Override public Integer visit(OpGraph graph) { return ReturningOpVisitorRouter.visit(this, graph.getSubOp()); }
        @Override public Integer visit(OpDistinct distinct) { return ReturningOpVisitorRouter.visit(this,distinct.getSubOp()); }
        @Override public Integer visit(OpSlice slice) { return ReturningOpVisitorRouter.visit(this,slice.getSubOp()); }
        @Override public Integer visit(OpOrder orderBy) { return ReturningOpVisitorRouter.visit(this,orderBy.getSubOp()); }
        @Override public Integer visit(OpProject project) { return ReturningOpVisitorRouter.visit(this,project.getSubOp()); }
        @Override public Integer visit(OpGroup groupBy) { return ReturningOpVisitorRouter.visit(this,groupBy.getSubOp()); }
        @Override public Integer visit(OpExtend extend) { return ReturningOpVisitorRouter.visit(this,extend.getSubOp()); }

        @Override
        public Integer visit(OpFilter filter) {
            return ReturningOpVisitorRouter.visit(this,filter.getSubOp());  // TODO could have service in filter EXISTS
        }

        @Override public Integer visit(OpSequence sequence) { return sequence.getElements().stream().mapToInt(o->ReturningOpVisitorRouter.visit(this,o)).sum(); }

        // TODO could have service in filter EXISTS

        @Override
        public Integer visit(Mj mj) { return mj.getElements().stream().mapToInt(o->ReturningOpVisitorRouter.visit(this,o)).sum(); }

        @Override
        public Integer visit(Mu mu) { return mu.getElements().stream().mapToInt(o->ReturningOpVisitorRouter.visit(this,o)).sum(); }

        @Override public Integer visit(OpLeftJoin lj) { return ReturningOpVisitorRouter.visit(this,lj.getLeft()) + ReturningOpVisitorRouter.visit(this,lj.getRight()); }
        @Override public Integer visit(OpConditional cond) { return ReturningOpVisitorRouter.visit(this,cond.getLeft()) + ReturningOpVisitorRouter.visit(this,cond.getRight()); }
        @Override public Integer visit(OpUnion union) { return ReturningOpVisitorRouter.visit(this,union.getLeft()) + ReturningOpVisitorRouter.visit(this,union.getRight()); }
        @Override public Integer visit(OpJoin join) { return ReturningOpVisitorRouter.visit(this,join.getLeft()) + ReturningOpVisitorRouter.visit(this,join.getRight()); }
    }

    public class CountNumberOfTriples extends ReturningOpVisitor<Integer> {

        @Override public Integer visit(OpService req) { return ReturningOpVisitorRouter.visit(this, req.getSubOp()); }

        @Override public Integer visit(OpTriple triple) { return 1; }
        @Override public Integer visit(OpQuad quad) { return 1; }
        @Override public Integer visit(OpQuadBlock block) { return block.getPattern().size(); }
        @Override public Integer visit(OpQuadPattern quads) { return quads.getPattern().size(); }
        @Override public Integer visit(OpBGP bgp) { return bgp.getPattern().size(); }
        @Override public Integer visit(OpTable table) { return 0; }

        @Override public Integer visit(OpGraph graph) { return ReturningOpVisitorRouter.visit(this, graph.getSubOp()); }
        @Override public Integer visit(OpDistinct distinct) { return ReturningOpVisitorRouter.visit(this,distinct.getSubOp()); }
        @Override public Integer visit(OpSlice slice) { return ReturningOpVisitorRouter.visit(this,slice.getSubOp()); }
        @Override public Integer visit(OpOrder orderBy) { return ReturningOpVisitorRouter.visit(this,orderBy.getSubOp()); }
        @Override public Integer visit(OpProject project) { return ReturningOpVisitorRouter.visit(this,project.getSubOp()); }
        @Override public Integer visit(OpGroup groupBy) { return ReturningOpVisitorRouter.visit(this,groupBy.getSubOp()); }
        @Override public Integer visit(OpExtend extend) { return ReturningOpVisitorRouter.visit(this,extend.getSubOp()); }

        @Override
        public Integer visit(OpFilter filter) {
            return ReturningOpVisitorRouter.visit(this,filter.getSubOp());  // TODO could have service in filter EXISTS
        }

        @Override public Integer visit(OpSequence sequence) { return sequence.getElements().stream().mapToInt(o->ReturningOpVisitorRouter.visit(this,o)).sum(); }

        // TODO could have service in filter EXISTS

        @Override
        public Integer visit(Mj mj) { return mj.getElements().stream().mapToInt(o->ReturningOpVisitorRouter.visit(this,o)).sum(); }

        @Override
        public Integer visit(Mu mu) {
            return mu.getElements().stream().mapToInt(o->ReturningOpVisitorRouter.visit(this,o)).sum();
        }

        @Override public Integer visit(OpLeftJoin lj) { return ReturningOpVisitorRouter.visit(this,lj.getLeft()) + ReturningOpVisitorRouter.visit(this,lj.getRight()); }
        @Override public Integer visit(OpConditional cond) { return ReturningOpVisitorRouter.visit(this,cond.getLeft()) + ReturningOpVisitorRouter.visit(this,cond.getRight()); }
        @Override public Integer visit(OpUnion union) { return ReturningOpVisitorRouter.visit(this,union.getLeft()) + ReturningOpVisitorRouter.visit(this,union.getRight()); }
        @Override public Integer visit(OpJoin join) { return ReturningOpVisitorRouter.visit(this,join.getLeft()) + ReturningOpVisitorRouter.visit(this,join.getRight()); }
    }

}
