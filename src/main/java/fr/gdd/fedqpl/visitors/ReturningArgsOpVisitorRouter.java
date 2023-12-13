package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Route the visitor to the proper one depending on the type of the `Op`
 * since it's not implemented in each `Op` itself. This probably lose some
 * performance, but it's not meant to be used intensively. This is the version
 * that includes a typed argument.
 */
public class ReturningArgsOpVisitorRouter {
    public static <R, A> R visit(ReturningArgsOpVisitor<R, A> t, Op op, A args) {
        return switch (op) {
            case Mu o -> t.visit(o, args);
            case Mj o -> t.visit(o, args);
            case OpService o -> t.visit(o, args);

            case OpTriple o -> t.visit(o, args);
            case OpQuad o -> t.visit(o, args);
            case OpBGP o -> t.visit(o, args);
            case OpSequence o -> t.visit(o, args);
            case OpTable o -> t.visit(o, args);
            case OpLeftJoin o -> t.visit(o, args);
            case OpConditional o -> t.visit(o, args);
            case OpFilter o -> t.visit(o, args);
            case OpDistinct o -> t.visit(o, args);
            case OpUnion o -> t.visit(o, args);
            case OpJoin o -> t.visit(o, args);

            case OpSlice o -> t.visit(o, args);
            case OpOrder o -> t.visit(o, args);
            case OpProject o -> t.visit(o, args);
            case OpGroup o -> t.visit(o, args);
            default -> throw new UnsupportedOperationException(op + "\nWith args: " + args.toString());
        };
    }
}
