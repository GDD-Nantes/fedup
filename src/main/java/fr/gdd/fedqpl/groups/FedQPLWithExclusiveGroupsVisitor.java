package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create exclusive groups when they are close from each other. It pushes
 * operators inside SERVICE clauses, so endpoints execute them themselves.
 * Execution and data are close from each other.
 *
 * TODO push-down FILTER, LIMIT, GROUP_BY etc.
 * TODO depending on an interface.
 */
public class FedQPLWithExclusiveGroupsVisitor extends ReturningOpBaseVisitor {

    public static boolean SILENT = true;

    @Override
    public Op visit(Mu mu) {
        ImmutablePair<List<OpService>, List<Op>> p = divide(mu.getElements());
        Map<Node, List<OpService>> groups = group(p.getLeft());

        // builds new nested unions
        List<OpService> newGroups = new ArrayList<>();
        for (Node uri : groups.keySet()) {
            if (groups.get(uri).size() <= 1) {
                newGroups.add(groups.get(uri).getFirst());
            } else {
                Op left = groups.get(uri).getFirst().getSubOp();
                for (int i = 1; i < groups.get(uri).size(); ++i) {
                    Op right = groups.get(uri).get(i).getSubOp();
                    left = OpUnion.create(left, right);
                }
                newGroups.add(new OpService(uri, left, SILENT));
            }
        }

        List<Op> ops = p.getRight().stream().map(o ->
                ReturningOpVisitorRouter.visit(this, o)).toList();

        List<Op> muChildren = new ArrayList<>();
        muChildren.addAll(newGroups);
        muChildren.addAll(ops);

        return new Mu(muChildren);
    }

    @Override
    public Op visit(Mj mj) {
        // TODO make sure to not create carthesian products
        ImmutablePair<List<OpService>, List<Op>> p = divide(mj.getElements());
        Map<Node, List<OpService>> groups = group(p.getLeft());

        // builds new joins
        List<OpService> newGroups = new ArrayList<>();
        for (Node uri : groups.keySet()) {
            if (groups.get(uri).size() <= 1) {
                newGroups.add(groups.get(uri).getFirst());
            } else {
                newGroups.add(new OpService(uri,
                        OpSequence.create().copy(groups.get(uri).stream().map(Op1::getSubOp).collect(Collectors.toList())),
                        SILENT));
            }
        }

        List<Op> ops = p.getRight().stream().map(o ->
                ReturningOpVisitorRouter.visit(this, o)).toList();

        List<Op> mjChildren = new ArrayList<>();
        mjChildren.addAll(newGroups);
        mjChildren.addAll(ops);

        return new Mj(mjChildren);
    }

    @Override
    public Op visit(OpConditional lj) {
        // check if left and right should be one big `Req` then merge
        // meaning they should have been simplified to the maximum beforehand.
        Op leftOp = lj.getLeft();
        Op rightOp = lj.getRight();

        if (rightOp instanceof OpService right && leftOp instanceof OpService left) {
            if (left.getService().equals(right.getService())) {
                return new OpService(left.getService(),
                        new OpConditional(left.getSubOp(), right.getSubOp()),
                        SILENT);
            }
        }
        // otherwise just run the thing inside each branch
        leftOp = ReturningOpVisitorRouter.visit(this, leftOp);
        rightOp = ReturningOpVisitorRouter.visit(this, rightOp);
        return new OpConditional(leftOp, rightOp);
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        // check if left and right should be one big `Req` then merge
        // meaning they should have been simplified to the maximum beforehand.
        Op leftOp = lj.getLeft();
        Op rightOp = lj.getRight();

        if (rightOp instanceof OpService right && leftOp instanceof OpService left) {
            if (left.getService().equals(right.getService())) {
                return new OpService(left.getService(),
                        OpLeftJoin.createLeftJoin(left.getSubOp(), right.getSubOp(), lj.getExprs()),
                        SILENT);
            }
        }
        // otherwise just run the thing inside each branch
        leftOp = ReturningOpVisitorRouter.visit(this, leftOp);
        rightOp = ReturningOpVisitorRouter.visit(this, rightOp);
        return OpLeftJoin.create(leftOp, rightOp, lj.getExprs());
    }


    /* ********************************************************************** */

    public static ImmutablePair<List<OpService>, List<Op>> divide(List<Op> children) {
        List<OpService> reqs = new ArrayList<>();
        List<Op> ops = new ArrayList<>();
        for (Op child : children) {
            if (child instanceof OpService) {
                reqs.add((OpService) child);
            } else {
                ops.add(child);
            }
        }
        return new ImmutablePair<>(reqs, ops);
    }

    // careful, the order might be different
    public static Map<Node, List<OpService>> group(List<OpService> toGroup) {
        Map<Node, List<OpService>> groups = new HashMap<>();
        for (OpService req: toGroup) {
            if (!groups.containsKey(req.getService()))
                groups.put(req.getService(), new ArrayList<>());
            groups.get(req.getService()).add(req);
        }
        return groups;
    }

}
