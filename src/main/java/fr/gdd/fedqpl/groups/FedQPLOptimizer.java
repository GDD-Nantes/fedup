package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.security.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Registers all optimizers of FedQPL expression and run them sequentially and loopingly
 * until the FedQPL expression converged or the timeout threshold is reached.
 */
public class FedQPLOptimizer {

    List<Supplier<ReturningOpBaseVisitor>> optimizers = new ArrayList<>();
    public Integer timeout = Integer.MAX_VALUE;

    public FedQPLOptimizer () {}

    public FedQPLOptimizer setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }

    public FedQPLOptimizer register(ReturningOpBaseVisitor optimizer) {
        this.optimizers.add(new Supplier<ReturningOpBaseVisitor>() {
            String name = optimizer.getClass().getSimpleName();

            @Override
            public ReturningOpBaseVisitor get() {
                return optimizer;
            }
        });
        return this;
    }

    public FedQPLOptimizer register(Supplier<ReturningOpBaseVisitor> optimizerSupplier) {
        this.optimizers.add(optimizerSupplier);
        return this;
    }

    public Op optimize(Op asFedQPL) {
        long deadline = System.currentTimeMillis() + timeout;
        Op before = null;
        while (System.currentTimeMillis() < deadline &&
                (Objects.isNull(before) ||
                !before.equalTo(asFedQPL, new NodeIsomorphismMap()))) { // should converge or timeout
            before = asFedQPL;
            for (Supplier<ReturningOpBaseVisitor> optimizerSupplier : optimizers) {
                Op beforeEach = null;
                while (System.currentTimeMillis() < deadline &&
                        (Objects.isNull(beforeEach) ||
                        !beforeEach.equalTo(asFedQPL, new NodeIsomorphismMap()))) { // should converge or timeout
                    beforeEach = asFedQPL;
                    asFedQPL = ReturningOpVisitorRouter.visit(optimizerSupplier.get(), asFedQPL);
                }
            }
        }
        return asFedQPL;
    }

}
