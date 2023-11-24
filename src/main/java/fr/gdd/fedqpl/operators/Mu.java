package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.*;

import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

/**
 * Multi-union operator.
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
