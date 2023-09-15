package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

/**
 * Multi-union operator.
 */
public class Mu extends FedQPLOperator {

    private Set<FedQPLOperator> children;

    public Mu() {
        this.children = new FedQPLOpSet();
    }

    public Mu(Set<FedQPLOperator> children) {
        this.children = children;
    }

    public void addChild(FedQPLOperator child) {
        this.children.add(child);
    }

    public void addChildren(Set<FedQPLOperator> children) {
        this.children.addAll(children);
    }

    public Set<FedQPLOperator> getChildren() {
        return children;
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return "Mu";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return this.children.hashCode() << 1 ^ getName().hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        // TODO Auto-generated method stub
        if (!(other instanceof Mu))
            return false;
        Mu opMu = (Mu) other;
        return opMu.children.equals(this.children);
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        if (!(opVisitor instanceof FedQPLVisitor)) {
            throw new IllegalArgumentException("The visitor should be an instance of FedQPLVisitor");
        }
        FedQPLVisitor visitor = (FedQPLVisitor) opVisitor;
        visitor.visit(this);
    };

    // @Override
    // public Query toSPARQL() {
    // Query q = QueryFactory.create();
    // q.
    // }

}
