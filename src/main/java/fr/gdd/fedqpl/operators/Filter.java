package fr.gdd.fedqpl.operators;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

public class Filter extends FedQPLOperator {
    
    private ExprList expressions;
    private FedQPLOperator subOp;

    public Filter(ExprList expressions, FedQPLOperator op) {
        this.expressions = expressions;
        this.subOp = op;
    }

    public ExprList getExprs() {
        return this.expressions;
    }

    public FedQPLOperator getSubOp() {
        return this.subOp;
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        // TODO Auto-generated method stub
        if (!(opVisitor instanceof FedQPLVisitor)) {
            throw new IllegalArgumentException("The visitor should be an instance of FedQPLVisitor");
        }
        FedQPLVisitor visitor = (FedQPLVisitor) opVisitor;
        subOp.visit(visitor);
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return "Filter";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return this.expressions.hashCode()<<1 ^ this.subOp.hashCode() ^ this.getName().hashCode();

    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        // TODO Auto-generated method stub
        if (!(other instanceof Filter))
            return false;
        Filter opFilter = (Filter) other;

        if (!opFilter.getExprs().equals(expressions, false)) {
            return false;
        }

        return opFilter.getSubOp().equalTo(subOp, labelMap);
    }

    // public Query toSPARQL(Query query){
    //     ElementTriplesBlock elmt_bgp = new ElementTriplesBlock();
    //     elmt_bgp.addTriple(triple);
    //     ElementNamedGraph elmt_graph = new ElementNamedGraph(source, elmt_bgp);

    //     if (query.getQueryPattern() == null) {
    //         query.setQueryPattern(elmt_graph);
    //     } else {
    //         ElementGroup elmt = (ElementGroup) query.getQueryPattern();
    //         elmt.addElement(elmt_graph);
    //         query.setQueryPattern(elmt);
    //     }

    //     return query;
    // }
}
