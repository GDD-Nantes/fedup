package fr.gdd.fedup.transforms;

import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;

/**
 * Important in some situation: `OpAsQuery` does not support `OpQuad`, but it
 * supports `OpQuadPattern`, so we transform each `OpQuad` into an `OpQuadPattern` of
 * 1 pattern.
 */
public class Quad2Pattern extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpQuad quad) {
        return new OpQuadPattern(quad.getQuad().getGraph(), quad.asQuadPattern().getBasicPattern());
    }

}
