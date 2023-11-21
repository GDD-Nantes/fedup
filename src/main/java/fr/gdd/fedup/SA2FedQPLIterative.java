package fr.gdd.fedup;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;

import java.util.List;
import java.util.Map;

/**
 * An incremental version of {@link SA2FedQPL}, i.e., the source selection
 * query returns a source assignment, how to build and integrate this new assignment
 * into the already built one.
 * TODO TODO TODO everything
 */
public class SA2FedQPLIterative implements FedQPLVisitorArg<Boolean, FedQPLOperator> {

    final Op query; // set in stone
    final ToQuadsTransform toQuads;
    FedQPLOperator root = new Mu();

    public SA2FedQPLIterative(Op query, ToQuadsTransform toQuads) {
        this.query = query;
        this.toQuads = toQuads;
    }

    public void addAssignment(Map<Var, String> assignment) {
        FedQPLOperator newBranch = SA2FedQPL.build(query, List.of(assignment), toQuads);

        newBranch.visit(this, this.root);
    }

    @Override
    public Boolean visit(Mu mu, FedQPLOperator arg) {
        if (arg instanceof Mu) {
            // continue exploring
            for (FedQPLOperator newChildren: mu.getChildren()) {
                for (FedQPLOperator oldChildren: ((Mu)arg).getChildren()) {
                    // TODO TODO TODO
                }
            }
        }

        return FedQPLVisitorArg.super.visit(mu, arg);
    }
}
