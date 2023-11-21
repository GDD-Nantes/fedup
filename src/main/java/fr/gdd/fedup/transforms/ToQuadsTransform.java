package fr.gdd.fedup.transforms;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.checkerframework.checker.units.qual.A;

import java.util.*;

/**
 * Adds Graph clauses to retrieves the necessary data to perform
 * source selection and create FedQPL expression.
 */
public class ToQuadsTransform extends TransformCopy {

    public final boolean asSequence = false; // join quads with opSequence or opJoin

    Integer nbGraphs = 0;
    Map<Var, Quad> var2quad = new HashMap<>();
    Map<Quad, Var> quad2var = new HashMap<>();

    Map<Triple, List<Pair<OpTriple, Var>>> triple2var = new HashMap<>();
    Map<OpBGP, Set<Var>> bgp2vars = new HashMap<>();

    public ToQuadsTransform() {}

    /**
     * Link variable and triple both ways in maps.
     * @param g The variable associated to the triple.
     * @param opTriple The triple associated to the variable.
     */
    public void add(Var g, OpTriple opTriple) {
        Quad quad = new Quad(g, opTriple.getTriple());
        var2quad.put(g, quad);
        quad2var.put(quad, g);
        if (!triple2var.containsKey(opTriple.getTriple())) {
            triple2var.put(opTriple.getTriple(), new ArrayList<>());
        }
        triple2var.get(opTriple.getTriple()).add(new ImmutablePair<>(opTriple, g));
    }

    public Var findVar(OpTriple opTriple) {
        return triple2var.get(opTriple.getTriple()).stream().filter(e -> e.getLeft() == opTriple).map(Pair::getRight).findFirst().orElse(null);
    }

    public Set<Var> findVars(OpBGP opBGP) {
        return bgp2vars.get(opBGP);
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
        this.add(g, opTriple);
        return new OpQuad(var2quad.get(g));
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<OpQuad> quads = opBGP.getPattern().getList().stream().map(triple ->
                (OpQuad) this.transform(new OpTriple(triple))
        ).toList();

        bgp2vars.put(opBGP, new HashSet<>());
        quads.forEach(q -> bgp2vars.get(opBGP).add((Var) q.getQuad().getGraph()));

        OpSequence sequence = OpSequence.create();
        quads.forEach(sequence::add);
        return sequence;
    }

}
