package fr.gdd.fedqpl.visitors;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Route the visitor to the proper one depending on the type of the `Op`
 * since it's not implemented in each `Op` itself. This probably lose some
 * performance, but it's not meant to be used intensively.
 */
public class ReturningOpVisitorRouter {
    public static <T> T visit(ReturningOpVisitor<T> t, Op op) {
        return switch (op) {
            case OpTriple o -> t.visit(o);
            case OpQuad o -> t.visit(o);
            case OpBGP o -> t.visit(o);
            case OpSequence o -> t.visit(o);
            case OpTable o -> t.visit(o);
            case OpLeftJoin o -> t.visit(o);
            case OpConditional o -> t.visit(o);
            case OpFilter o -> t.visit(o);
            case OpDistinct o -> t.visit(o);
            case OpUnion o -> t.visit(o);
            case OpJoin o -> t.visit(o);
            default -> throw new UnsupportedOperationException(op.toString());
        };
    }
}
