package fr.gdd.fedup.transforms;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AddFilterForAskedGraphs extends TransformCopy {

    ToValuesAndOrderTransform valuesAndOrder;

    public Set<ExprList> askFilters = new HashSet<>(); // filters that must be kept by the summarizer

    public AddFilterForAskedGraphs(ToValuesAndOrderTransform valuesAndOrder) {
        this.valuesAndOrder = valuesAndOrder;
    }

    @Override
    public Op transform(OpQuad opQuad) {
        Var g = (Var) opQuad.getQuad().getGraph();
        Triple triple = opQuad.getQuad().asTriple();

        if (valuesAndOrder.values2quad.containsValue(opQuad) ||
                !valuesAndOrder.triple2Endpoints.containsKey(triple)) {
            return opQuad; // do nothing john snow
        }

        OpFilter op = (OpFilter) prepareFilter(valuesAndOrder.triple2Endpoints.get(triple), opQuad, g);
        askFilters.add(op.getExprs());
        return op;
    }

    public static Op prepareFilter(List<String> endpoints, Op op, Var graph) {
        List<Expr> exprs = endpoints.stream().map(e -> ExprUtils.parse(String.format("%s = <%s>", graph, e))).toList();
        Expr expr = switch (exprs.size()) {
            case 1 -> exprs.getFirst();
            case 2 -> new E_LogicalOr(exprs.getFirst(), exprs.getLast()); // masterclass :)
            default -> {
                Expr left = exprs.getFirst();
                for (int i = 1; i < exprs.size(); ++i ) {
                    Expr right = exprs.get(i);
                    left = new E_LogicalOr(left, right);
                }
                yield left;
            }
        };

        return OpFilter.filterBy(new ExprList(expr), op);
    }
}
