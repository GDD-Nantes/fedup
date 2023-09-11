package fr.gdd.fedup.summary;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;

/**
 * Transform the SPARQL
 */
public class HashQueryTransformer extends QueryTransformer {

    Integer modulo = 1;

    public HashQueryTransformer(Integer modulo) {
        super(true);
        this.modulo = modulo;
    }

    @Override
    public Op transform(OpTriple opTriple) {
        Triple triple = HashSummarizer.summarize(opTriple.getTriple(), modulo);
        return new OpTriple(triple);
    }

    @Override
    public Op transform(OpQuad opQuad) {
        Quad quad = HashSummarizer.summarize(opQuad.getQuad(), modulo);
        return new OpQuad(quad);
    }

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        return Transformer.transform(this, subOp); // TODO: handle special filter expressions, i.e., we don't want to remove simple equalities
    }
}
