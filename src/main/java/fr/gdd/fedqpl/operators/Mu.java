package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
}
