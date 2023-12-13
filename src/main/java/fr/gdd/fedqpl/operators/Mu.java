package fr.gdd.fedqpl.operators;

import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.List;
import java.util.Objects;

/**
 * Multi-union operator that does not seem to exist in Apache Jena's list of `Op`.
 * The children is a list to keep the possibility of ordering. It may improve the
 * query execution efficiency in presence of LIMIT.
 */
public class Mu extends OpN {

    public Mu() {}

    public Mu(List<Op> children) {
        this.addChildren(children);
    }

    public Mu addChild(Op child) {
        this.getElements().add(child);
        return this;
    }

    public Mu addChildren(List<Op> children) {
        children.forEach(this::addChild);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName() + getElements().stream().map(Op::hashCode)
                .reduce(0, Integer::sum));
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if ( ! ( other instanceof Mu) ) return false;
        Mu otherMu = (Mu) other;
        return super.equalsSubOps(otherMu, labelMap);
    }

    @Override
    public String getName() {
        return "mu";
    }

    @Override
    public String toString(PrefixMapping pmap) {
        return String.format("({}\n{})", getName(),
                String.join("\n", getElements().stream().map(c -> c.toString(pmap)).toList()));
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
        return new Mu(elts);
    }

}
