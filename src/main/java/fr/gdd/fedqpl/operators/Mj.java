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
public class Mj implements FedQPLOperator {
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
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
