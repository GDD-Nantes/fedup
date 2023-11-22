package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;

import java.util.List;

public class Project extends OpProject implements FedQPLOperator {

    FedQPLOperator child;

    public Project(List<Var> vars) {
        super(null, vars);
    }

    public FedQPLOperator getChild() {
        return child;
    }

    public Project setChild(FedQPLOperator child) {
        this.child = child;
        return this;
    }

    @Override
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <T,S> T visit(FedQPLVisitorArg<T,S> visitor, S arg) {
        return visitor.visit(this, arg);
    }
}
