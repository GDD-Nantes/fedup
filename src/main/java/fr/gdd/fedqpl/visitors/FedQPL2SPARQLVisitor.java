package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.Iterator;

/**
 * Converts a FedQPL expression into a SPARQL {@link Op} service query. To get its
 * String version, please consider using `OpAsQuery.asQuery(op).toString()`.
 */
public class FedQPL2SPARQLVisitor implements FedQPLVisitor<Op> {

    @Override
    public Op visit(Req req) {
        OpService service = new OpService(req.getSource(),
                new OpBGP(BasicPattern.wrap(req.getTriples())),
                false); // false : if the service returns an error, the whole query fails
        return service;
    }

    @Override
    public Op visit(Mu mu) {
        return switch (mu.getChildren().size()) {
            case 0 -> OpNull.create();
            case 1 -> mu.getChildren().iterator().next().visit(this);
            default -> {
                // wrote as nested unions
                Iterator<FedQPLOperator> ops = mu.getChildren().iterator();
                Op left = ops.next().visit(this);
                while (ops.hasNext()) {
                    Op right = ops.next().visit(this);
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

    @Override
    public Op visit(Mj mj) {
        return switch (mj.getChildren().size()) {
            case 0 -> OpNull.create();
            case 1 -> mj.getChildren().iterator().next().visit(this);
            default -> {
                // as nested joins
                Iterator<FedQPLOperator> ops = mj.getChildren().iterator();
                Op left = ops.next().visit(this);
                while (ops.hasNext()) {
                    Op right = ops.next().visit(this);
                    left = OpJoin.create(left, right);
                }
                yield left;
            }
        };
    }

    @Override
    public Op visit(LeftJoin lj) {
        return new OpConditional(
                lj.getLeft().visit(this),
                lj.getRight().visit(this));
    }

    @Override
    public Op visit(Filter filter) {
        return OpFilter.filterBy(
                filter.getExprs(),
                filter.getSubOp().visit(this));
    }
}
