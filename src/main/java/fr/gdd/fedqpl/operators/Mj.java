package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.HashSet;
import java.util.Set;

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

    public Object visit(FedQPLVisitor visitor, Object args) {
        return visitor.visit(this, args);
    }
}
