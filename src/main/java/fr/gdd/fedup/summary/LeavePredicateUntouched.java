package fr.gdd.fedup.summary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;

/**
 * Abstract class that call the transform of nodes except for the predicate.
 */
public abstract class LeavePredicateUntouched extends TransformCopy {

    abstract Node transform(Node node);

    @Override
    public Op transform(OpTriple opTriple) {
        Node subject = this.transform(opTriple.getTriple().getSubject());
        Node predicate = opTriple.getTriple().getPredicate();
        Node object = this.transform(opTriple.getTriple().getObject());
        return new OpTriple(Triple.create(subject, predicate, object));
    }

    @Override
    public Op transform(OpQuad opQuad) {
        Node graph = opQuad.getQuad().getGraph();
        Node subject = this.transform(opQuad.getQuad().getSubject());
        Node predicate = opQuad.getQuad().getPredicate();
        Node object = this.transform(opQuad.getQuad().getObject());
        return new OpQuad(Quad.create(graph, subject, predicate, object));
    }

    @Override
    public Op transform(OpBGP opBGP) {
        OpBGP transformed = new OpBGP();
        for (Triple t : opBGP.getPattern().getList()) {
            OpTriple opTriple = (OpTriple) Transformer.transform(this, new OpTriple(t));
            transformed.getPattern().add(opTriple.getTriple());
        }
        return transformed;
    }
}
