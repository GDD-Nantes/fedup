package fr.gdd.fedup.transforms;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpTriple;

public class RemoveGraphsTransform extends TransformCopy {

    @Override
    public Op transform(OpQuad opQuad) {
        return new OpTriple(opQuad.getQuad().asTriple());
    }

    @Override
    public Op transform(OpQuadPattern opQuadPattern) {
        return new OpBGP(opQuadPattern.getBasicPattern());
    }
}
