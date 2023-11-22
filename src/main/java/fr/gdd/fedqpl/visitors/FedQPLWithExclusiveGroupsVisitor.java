package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Create exclusive groups when they are close from each other
 */
public class FedQPLWithExclusiveGroupsVisitor implements FedQPLVisitor<FedQPLOperator> {

    @Override
    public FedQPLOperator visit(Mu mu) {
        ImmutablePair<List<Req>, List<FedQPLOperator>> p = divide(mu.getChildren());
        Map<Node, List<Req>> groups = group(p.getLeft());

        // builds new nested unions
        List<Req> newGroups = new ArrayList<>();
        for (Node uri : groups.keySet()) {
            if (groups.get(uri).size() <= 1) {
                newGroups.add(groups.get(uri).getFirst());
            } else {
                Op left = groups.get(uri).getFirst().getOp();
                for (int i = 1; i < groups.get(uri).size(); ++i) {
                    Op right = groups.get(uri).get(i).getOp();
                    left = OpUnion.create(left, right);
                }
                newGroups.add(new Req(left, uri));
            }
        }

        List<FedQPLOperator> ops = p.getRight().stream().map(o -> o.visit(this)).toList();

        List<FedQPLOperator> muChildren = new ArrayList<>();
        muChildren.addAll(newGroups);
        muChildren.addAll(ops);

        return new Mu(muChildren);
    }

    @Override
    public FedQPLOperator visit(Mj mj) {
        ImmutablePair<List<Req>, List<FedQPLOperator>> p = divide(mj.getChildren());
        Map<Node, List<Req>> groups = group(p.getLeft());

        // builds new joins
        List<Req> newGroups = new ArrayList<>();
        for (Node uri : groups.keySet()) {
            if (groups.get(uri).size() <= 1) {
                newGroups.add(groups.get(uri).getFirst());
            } else {
                newGroups.add(new Req(OpSequence.create().copy(groups.get(uri).stream().map(Req::getOp).collect(Collectors.toList())), uri));
            }
        }

        List<FedQPLOperator> ops = p.getRight().stream().map(o -> o.visit(this)).toList();

        List<FedQPLOperator> mjChildren = new ArrayList<>();
        mjChildren.addAll(newGroups);
        mjChildren.addAll(ops);

        return new Mj(mjChildren);
    }

    @Override
    public FedQPLOperator visit(Req req) {
        return req; // do nothing
    }

    @Override
    public FedQPLOperator visit(LeftJoin lj) {
        // check if left and right should be one big `Req` then merge
        // meaning they should have been simplified to the maximum beforehand.
        FedQPLOperator leftOp = lj.getLeft();
        FedQPLOperator rightOp = lj.getRight();

        leftOp = leftOp.visit(new FedQPLSimplifyVisitor());
        rightOp = rightOp.visit(new FedQPLSimplifyVisitor());

        if (rightOp instanceof Req && leftOp instanceof Req) {
            Req left = (Req) rightOp;
            Req right = (Req) leftOp;

            if (left.getSource().equals(right.getSource())) {
                return new Req(new OpConditional(left.getOp(), right.getOp()), left.getSource());
            }
        }
        // otherwise just run the thing inside each branch
        return new LeftJoin(leftOp.visit(this), rightOp.visit(this));
    }

    @Override
    public FedQPLOperator visit(Filter filter) {
        return new Filter(filter.getExprs(), filter.getSubOp().visit(this));
    }

    @Override
    public FedQPLOperator visit(Limit limit) {
        // TODO LIMIT push down, so it could be part of the query sent to endpoints.
        return new Limit(limit.getStart(), limit.getLength()).setChild(limit.getChild().visit(this));
    }

    @Override
    public FedQPLOperator visit(OrderBy orderBy) {
        // TODO ORDERBY push down, so it could be part of the query sent to endpoints.
        return new OrderBy(orderBy.getConditions()).setChild(orderBy.getChild().visit(this));
    }

    @Override
    public FedQPLOperator visit(Project project) {
        // TODO PROJECT push down, so it could be part of the query sent to endpoints.
        return new Project(project.getVars()).setChild(project.getChild().visit(this));
    }

    /* ********************************************************************** */

    public static ImmutablePair<List<Req>, List<FedQPLOperator>> divide(List<FedQPLOperator> children) {
        List<Req> reqs = new ArrayList<>();
        List<FedQPLOperator> ops = new ArrayList<>();
        for (FedQPLOperator child : children) {
            if (child instanceof Req) {
                reqs.add((Req) child);
            } else {
                ops.add(child);
            }
        }
        return new ImmutablePair<>(reqs, ops);
    }

    // careful, the order might be different
    public static Map<Node, List<Req>> group(List<Req> toGroup) {
        Map<Node, List<Req>> groups = new HashMap<>();
        for (Req req: toGroup) {
            if (!groups.containsKey(req.getSource()))
                groups.put(req.getSource(), new ArrayList<>());
            groups.get(req.getSource()).add(req);
        }
        return groups;
    }

}
