package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.OpCloningUtil;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FactorizeUnionsOfLeftJoinsVisitor extends ReturningOpBaseVisitor {

    public Integer nbValues = 0;
    public static final String VAR = "_fuoljv_";

    public FactorizeUnionsOfLeftJoinsVisitor() {}

    public FactorizeUnionsOfLeftJoinsVisitor(Integer nbValues) {
        this.nbValues = nbValues;
    }

    @Override
    public Op visit(OpService req) {
        return req; // we don't dive into req
    }

    @Override
    public Op visit(Mu mu) {
        // #A split between service and the rest
        List<OpLeftJoin> candidates = new ArrayList<>();
        List<Op> rest = new ArrayList<>();
        for (Op op : mu.getElements()) {
            // TODO possibly do not check if OpService, Mu is still ok + other visitor will create values of req.
            if (op instanceof OpLeftJoin opLeftJoin && opLeftJoin.getLeft() instanceof OpService) {
                candidates.add(opLeftJoin);
            } else {
                rest.add(op);
            }
        }

        // #B group identical services
        Map<OpLeftJoin, List<Node>> subOpToEndpoints = new HashMap<>();
        for (OpLeftJoin candidate : candidates) {
            boolean found = false;
            for (OpLeftJoin comparedWith : subOpToEndpoints.keySet()) {
                if (candidate.getRight().equalTo(comparedWith.getRight(), new NodeIsomorphismMap()) &&
                        ((OpService)candidate.getLeft()).getSubOp().equalTo(((OpService)comparedWith.getLeft()).getSubOp(), new NodeIsomorphismMap())) {
                    subOpToEndpoints.get(comparedWith).add(((OpService)candidate.getLeft()).getService());
                    found = true;
                    break;
                }
            }
            if (!found) {
                List<Node> endpoints = new ArrayList<>();
                endpoints.add(((OpService)candidate.getLeft()).getService());
                subOpToEndpoints.put(candidate, endpoints);
            }
        }

        // #C rewrite the union of services as union of values
        Mu newMu = new Mu();
        newMu.addChildren(rest.stream().map(r -> ReturningOpVisitorRouter.visit(new FactorizeUnionsOfLeftJoinsVisitor(this.nbValues), r)).collect(Collectors.toList()));

        subOpToEndpoints.forEach((op, nodes) -> {
            if (nodes.size() == 1) {
                newMu.add(op);
            } else {
                nbValues += 1;
                Var valuesVar = Var.alloc(VAR + nbValues);
                TableN tableN = new TableN(List.of(valuesVar));
                nodes.forEach(n -> tableN.addBinding(BindingFactory.binding(valuesVar, n)));
                OpTable values = OpTable.create(tableN);
                OpService serviceWithVariable = new OpService(valuesVar, ((OpService)op.getLeft()).getSubOp(), true);
                Op sequence = OpSequence.create(values, serviceWithVariable);
                OpLeftJoin newLeftJoin = OpCloningUtil.clone(op, sequence, op.getRight());
                newMu.add(newLeftJoin);
            }
        });

        return newMu.size() > 1 ? newMu: newMu.get(0);
    }

}
