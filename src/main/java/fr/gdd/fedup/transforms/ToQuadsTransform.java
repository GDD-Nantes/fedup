package fr.gdd.fedup.transforms;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds Graph clauses to retrieves the necessary data to perform
 * source selection and create FedQPL expression.
 */
public class ToQuadsTransform extends TransformCopy {

    public final boolean asSequence = false; // join quads with opSequence or opJoin

    Integer nbGraphs = 0;
    Map<Var, Quad> var2quad = new HashMap<>();
    Map<Quad, Var> quad2var = new HashMap<>();

    public ToQuadsTransform() {}

    /**
     * Link variable and triple both ways in maps.
     * @param var The variable associated to the triple.
     * @param quad The quad associated to the variable.
     */
    public void add(Var var, Quad quad) {
        var2quad.put(var, quad);
        quad2var.put(quad, var);
    }

    /**
     * @return The set of new vars dedicated to graph selection and their associated triple.
     */
    public Map<Var, Quad> getVar2quad() { return var2quad; }

    /**
     * @return The var associated to the triple.
     */
    public Map<Quad, Var> getQuad2var() { return quad2var; }

    @Override
    public Op transform(OpTriple opTriple) {
        nbGraphs += 1;
        Var g = Var.alloc("g" + nbGraphs);
        Quad quad = new Quad(g, opTriple.getTriple());
        this.add(g, quad);
        return new OpQuad(quad);
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Op> quads = opBGP.getPattern().getList().stream().map(triple ->
                this.transform(new OpTriple(triple))
        ).toList();
        OpSequence sequence = OpSequence.create();
        quads.forEach(sequence::add);
        return sequence;
    }

}
