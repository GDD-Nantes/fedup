package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.OpCloningUtil;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.http.client.utils.CloneUtils;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpService;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Apply basic rewritings to simplify the FedQPL expression. It removes
 * multi-unions or multi-joins with less than 2 children.
 */
public class FedQPLSimplifyVisitor extends ReturningOpBaseVisitor {

    @Override
    public Op visit(Mj mj) {
        List<Op> children = mj.getElements().stream().map(c ->
                ReturningOpVisitorRouter.visit(this, c)).collect(Collectors.toList());
        if (children.isEmpty()) {
            return null;
        } else if (children.size() == 1) {
            return children.getFirst();
        } else {
            return new Mj(children);
        }
    }

    @Override
    public Op visit(Mu mu) {
        List<Op> children = mu.getElements().stream().map(c ->
                ReturningOpVisitorRouter.visit(this, c)).collect(Collectors.toList());
        if (children.isEmpty()) {
            return null;
        } else if (children.size() == 1) {
            return children.getFirst();
        } else {
            return new Mu(children);
        }
    }

    @Override
    public Op visit(OpConditional lj) {
        Op left = ReturningOpVisitorRouter.visit(this, lj.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, lj.getRight());

        if (Objects.isNull(left)) {
            return null; // mandatory part is not even returning stuff
        }
        if (Objects.isNull(right)) {
            return left;
        }
        return OpCloningUtil.clone(lj, left, right);
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        Op left = ReturningOpVisitorRouter.visit(this, lj.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, lj.getRight());

        if (Objects.isNull(left)) {
            return null; // mandatory part is not even returning stuff
        }
        if (Objects.isNull(right)) {
            return left; // filter in lj disappears as well
        }
        return OpCloningUtil.clone(lj, left, right);
    }
}
