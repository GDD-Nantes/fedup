package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Registers all optimizers of FedQPL expression and run them sequentially and loopingly
 * until the FedQPL expression converged or the timeout threshold is reached.
 */
public class FedQPLOptimizer {

    List<ReturningOpBaseVisitor> optimizers = new ArrayList<>();
    public Integer timeout = Integer.MAX_VALUE;

    public FedQPLOptimizer () {}

    public FedQPLOptimizer setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }

    public FedQPLOptimizer register(ReturningOpBaseVisitor optimizer) {
        this.optimizers.add(optimizer);
        return this;
    }

    public Op optimize(Op asFedQPL) {
        long deadline = System.currentTimeMillis() + timeout;
        Op before = null;
        while (System.currentTimeMillis() < deadline &&
                (Objects.isNull(before) ||
                !before.equalTo(asFedQPL, new NodeIsomorphismMap()))) { // should converge or timeout
            before = asFedQPL;
            for (ReturningOpBaseVisitor optimizer : optimizers) {
                Op beforeEach = null;
                while (System.currentTimeMillis() < deadline &&
                        (Objects.isNull(beforeEach) ||
                        !beforeEach.equalTo(asFedQPL, new NodeIsomorphismMap()))) { // should converge or timeout
                    beforeEach = asFedQPL;
                    asFedQPL = ReturningOpVisitorRouter.visit(optimizer, asFedQPL);
                }
            }
        }
        return asFedQPL;
    }

}
