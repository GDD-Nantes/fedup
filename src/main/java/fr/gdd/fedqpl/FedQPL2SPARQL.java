package fr.gdd.fedqpl;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.Iterator;

/**
 * Converts a FedQPL expression into a SPARQL {@link Op} service query. To get its
 * String version, please consider using `OpAsQuery.asQuery(op).toString()`.
 * It mainly consists in converting multi-unions and multi-joins into Apache Jena
 * operators.
 */
public class FedQPL2SPARQL extends ReturningOpBaseVisitor {

    @Override
    public Op visit(Mu mu) {
        return switch (mu.getElements().size()) {
            case 0 -> OpNull.create();
            case 1 -> ReturningOpVisitorRouter.visit(this, mu.getElements().iterator().next());
            default -> {
                // wrote as nested unions
                Iterator<Op> ops = mu.getElements().iterator();
                Op left = ReturningOpVisitorRouter.visit(this, ops.next());
                while (ops.hasNext()) {
                    Op right = ReturningOpVisitorRouter.visit(this, ops.next());
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

    @Override
    public Op visit(Mj mj) {
        return switch (mj.getElements().size()) {
            case 0 -> OpNull.create();
            case 1 -> ReturningOpVisitorRouter.visit(this, mj.getElements().iterator().next());
            default -> {
                // as nested joins
                Iterator<Op> ops = mj.getElements().iterator();
                Op left = ReturningOpVisitorRouter.visit(this, ops.next());
                while (ops.hasNext()) {
                    Op right = ReturningOpVisitorRouter.visit(this, ops.next());
                    left = OpJoin.create(left, right);
                }
                yield left;
            }
        };
    }

}
