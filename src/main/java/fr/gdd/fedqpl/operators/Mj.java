package fr.gdd.fedqpl.operators;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Multi-join operator, basically a sequence operator.
 * Children are in a list even though the order does not necessarily matter,
 * to keep the possibility of join ordering if need be.
 */
public class Mj extends OpN {

    List<Op> children = new ArrayList<>();

    public Mj() {}

    public Mj(List<Op> children) {
        this.addChildren(children);
    }

    public Mj addChild(Op child) {
        this.getElements().add(child);
        return this;
    }

    public Mj addChildren(List<Op> children) {
        this.getElements().addAll(children);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName() + getElements().stream().map(Op::hashCode)
                .reduce(0, Integer::sum));
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if ( ! ( other instanceof Mj) ) return false;
        Mj otherMj = (Mj) other;
        return super.equalsSubOps(otherMj, labelMap);
    }

    @Override
    public String getName() {
        return "mj";
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op apply(Transform transform, List<Op> elts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OpN copy(List<Op> elts) {
        return new Mj(elts);
    }

}
