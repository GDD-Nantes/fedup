package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

/**
 * Multi-join operator.
 */
public class Mj extends FedQPLOperator {
    Set<FedQPLOperator> children = new HashSet<>();

    public Mj() {}

    public Mj(Set<FedQPLOperator> children) {
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
    public void visit(OpVisitor opVisitor) {
        // TODO Auto-generated method stub
        if (!(opVisitor instanceof FedQPLVisitor)) {
            throw new IllegalArgumentException("The visitor should be an instance of FedQPLVisitor");
        }
        FedQPLVisitor visitor = (FedQPLVisitor) opVisitor;
        visitor.visit(this);
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return "Mj";
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
        Mj opMj = (Mj) other;
        return opMj.children.equals(this.children);
    }
}
