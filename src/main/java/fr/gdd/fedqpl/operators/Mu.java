package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.*;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

/**
 * Multi-union operator.
 */
public class Mu implements FedQPLOperator {

    private List<FedQPLOperator> children;

    public Mu() {
        this.children = new ArrayList();
    }

    public Mu(List<FedQPLOperator> children) {
        this.children = children;
    }

    public void addChild(FedQPLOperator child) {
        this.children.add(child);
    }

    public void addChildren(Set<FedQPLOperator> children) {
        this.children.addAll(children);
    }

    public List<FedQPLOperator> getChildren() {
        return children;
    }

    @Override
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mu mu = (Mu) o;
        return Objects.equals(children, mu.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(children);
    }
}
