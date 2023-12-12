package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.Table1;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Aims to factorize common parts of the FedQPL tree with VALUES. The representation
 * is more compact, and therefore more human friendly. It is worth noting that such
 * a representation may not be machine friendly. For instance, Virtuoso does not
 * like `… values (?g) … service ?g { … }`
 */
public class FactorizeVisitor extends ReturningOpBaseVisitor {

    public Integer nbValues = 0;

    @Override
    public Op visit(Mu mu) {
        // #A split between service and the rest
        List<OpService> candidates = new ArrayList<>();
        List<Op> rest = new ArrayList<>();
        for (Op op : mu.getElements()) {
            if (op instanceof OpService opService) {
                candidates.add(opService);
            } else {
                rest.add(op);
            }
        }

        // #B group identical services
        Map<Op, List<Node>> subOpToEndpoints = new HashMap<>();
        for (OpService candidate : candidates) {
            boolean found = false;
            for (Op comparedWith : subOpToEndpoints.keySet()) {
                if (candidate.getSubOp().equalTo(comparedWith, new NodeIsomorphismMap())) {
                    subOpToEndpoints.get(comparedWith).add(candidate.getService());
                    found = true;
                    break;
                }
            }
            if (!found) {
                List<Node> endpoints = new ArrayList<>();
                endpoints.add(candidate.getService());
                subOpToEndpoints.put(candidate.getSubOp(), endpoints);
            }
        }

        // #C rewrite the union of services as union of values
        Mu newMu = new Mu();
        newMu.addChildren(rest.stream().map(r -> ReturningOpVisitorRouter.visit(this, r)).collect(Collectors.toList()));

        subOpToEndpoints.forEach((op, nodes) -> {
            if (nodes.size() == 1){
                newMu.add(new OpService(nodes.get(0), op, true));
            } else {
                nbValues += 1;
                Var valuesVar = Var.alloc("_v_" + nbValues);
                TableN tableN = new TableN(List.of(valuesVar));
                nodes.forEach(n -> tableN.addBinding(BindingFactory.binding(valuesVar, n)));
                OpTable values = OpTable.create(tableN);
                OpService serviceWithVariable = new OpService(valuesVar, op, true);
                Op sequence = OpSequence.create(values, serviceWithVariable);
                newMu.add(sequence);
            }
         });

        return newMu.size() > 1 ? newMu: newMu.get(0);
    }
}
