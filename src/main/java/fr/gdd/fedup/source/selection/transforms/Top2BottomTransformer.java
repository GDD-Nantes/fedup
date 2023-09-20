package fr.gdd.fedup.source.selection.transforms;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Since `Transformer.transform` starts from the bottom, it's difficult to build
 * meaningful result between operators.
 */
public class Top2BottomTransformer {

    public static Op transform(Transform t, Op op) {
        Op result = switch (op) {
            case OpTriple o -> t.transform(o);
            case OpQuad o -> t.transform(o);
            case OpBGP o -> t.transform(o);
            case OpSequence o -> t.transform(o, null);
            case OpTable o -> t.transform(o);
            case OpLeftJoin o -> t.transform(o, null, null);
            case OpConditional o -> t.transform(o, null, null);
            case OpFilter o -> t.transform(o, null);
            default -> throw new UnsupportedOperationException(op.toString());
        };
        return result;
    }

}
