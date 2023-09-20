package fr.gdd.fedup.source.selection.transforms;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds Graph clauses to retrieves the necessary data to perform
 * source selection and create FedQPL expression.
 */
public class ToQuadsTransform extends TransformCopy {

    Integer nbGraphs = 0;
    Map<Var, Triple> var2Triple = new HashMap<>();
    Map<Triple, Var> triple2Var = new HashMap<>();

    public ToQuadsTransform() {}

    /**
     * Link variable and triple both ways in maps.
     * @param var The variable associated to the triple.
     * @param triple The triple associated to the variable.
     */
    private void add(Var var, Triple triple) {
        var2Triple.put(var, triple);
        triple2Var.put(triple, var);
    }

    /**
     * @return The set of new vars dedicated to graph selection and their associated triple.
     */
    public Map<Var, Triple> getVar2Triple() { return var2Triple; }

    /**
     * @return The var associated to the triple.
     */
    public Map<Triple, Var> getTriple2Var() { return triple2Var; }

    @Override
    public Op transform(OpTriple opTriple) {
        nbGraphs += 1;
        Var g = Var.alloc("g" + nbGraphs);
        this.add(g, opTriple.getTriple());
        Quad quad = new Quad(g, opTriple.getTriple());
        return new OpQuad(quad);
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Op> quads = opBGP.getPattern().getList().stream().map(triple ->
            Transformer.transform(this, new OpTriple(triple))
        ).toList();

        if (quads.size() == 1) {
            return quads.get(0);
        } else {
            Op op = OpJoin.create(quads.get(0), quads.get(1));
            for (int i = 2; i < quads.size(); i++) {
                op = OpJoin.create(op, quads.get(i));
            }
            return op;
        }
    }

    @Override
    public Op transform(OpProject opProject, Op subOp) {
        return Transformer.transform(this, subOp); // no OpProject <=> SELECT * WHERE…
    }

    @Override
    public Op transform(OpSlice opSlice, Op subOp) { // removes LIMITs
        return Transformer.transform(this, subOp);
    }
}
