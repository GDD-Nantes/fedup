package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpService;

import java.util.ArrayList;
import java.util.List;

/**
 * Aims to factorize common parts of the FedQPL tree with VALUES.
 */
public class FactorizeVisitor extends ReturningOpBaseVisitor {

    @Override
    public Op visit(Mu mu) {
        List<OpService> candidates = new ArrayList<>();
        for (Op op : mu.getElements()) {
            // TODO TODO TODO

        }
        //return super.visit(mu);
        return null;
    }
}
