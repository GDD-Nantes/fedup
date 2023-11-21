package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Multi-join operator.
 */
public class Mj implements FedQPLOperator {
    /**
     * Children are in a list even though the order does not necessarily matter,
     * to keep the possibility of join ordering if need be.
     */
    List<FedQPLOperator> children = new ArrayList<>();

    public Mj() {}

    public Mj(List<FedQPLOperator> children) {
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
        Mj mj = (Mj) o;
        return Objects.equals(children, mj.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(children);
    }
}
