package fr.gdd.fedup.transforms;

import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;

/**
 * Removes sequence operators.
 */
public class RemoveSequences extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpSequence sequence) {
        return switch (sequence.size()) {
            case 0 -> sequence;
            case 1 -> sequence.get(0);
            default -> {
                Op left = ReturningOpVisitorRouter.visit(this, sequence.get(0));
                for (int i = 1; i < sequence.size(); ++i) {
                    Op right = ReturningOpVisitorRouter.visit(this, sequence.get(i));
                    left = OpJoin.create(left, right);
                }
                yield left;
            }
        };
    }
}
