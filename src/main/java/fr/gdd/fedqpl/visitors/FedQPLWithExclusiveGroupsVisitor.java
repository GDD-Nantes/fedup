package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Node;
import org.checkerframework.checker.units.qual.A;

import java.util.*;
import java.util.stream.Collectors;

public class FedQPLWithExclusiveGroupsVisitor implements FedQPLVisitor<FedQPLOperator> {

    @Override
    public FedQPLOperator visit(Mu mu) {
        // TODO check if union is inside a big Req when Req store List<Op>
        return FedQPLVisitor.super.visit(mu);
    }

    @Override
    public FedQPLOperator visit(Mj mj) {
        ImmutablePair<List<Req>, List<FedQPLOperator>> p = divide(mj.getChildren());
        List<Req> groups = group(p.getLeft());
        List<FedQPLOperator> ops = p.getRight().stream().map(o -> o.visit(this)).collect(Collectors.toList());

        Set<FedQPLOperator> mjChildren = new HashSet<>();
        mjChildren.addAll(groups);
        mjChildren.addAll(ops);

        return new Mj(mjChildren);
    }

    @Override
    public FedQPLOperator visit(Req req) {
        return req; // do nothing
    }

    @Override
    public FedQPLOperator visit(LeftJoin lj) {
        // TODO check if leftjoin is inside a big Req when Req store List<Op>
        return new LeftJoin(lj.getLeft().visit(this), lj.getRight().visit(this));
    }

    @Override
    public FedQPLOperator visit(Filter filter) {
        return new Filter(filter.getExprs(), filter.getSubOp().visit(this));
    }


    /* ********************************************************************** */

    public static ImmutablePair<List<Req>, List<FedQPLOperator>> divide(Set<FedQPLOperator> children) {
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
    public static List<Req> group(List<Req> toGroup) {
        Map<Node, List<Req>> groups = new HashMap<>();
        for (Req req: toGroup) {
            if (!groups.containsKey(req.getSource()))
                groups.put(req.getSource(), new ArrayList<>());
            groups.get(req.getSource()).add(req);
        }
        return groups.entrySet().stream().map(e->
            new Req(e.getValue().stream().map(r-> r.getTriples()).flatMap(List::stream).collect(Collectors.toList()),
                    e.getKey())).collect(Collectors.toList());
    }

}
