package fr.gdd.fedup.source.selection.asks;

import fr.gdd.raw.io.OpVisitorUnimplemented;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Visitor that collects all triples to perform parallel asks.
 */
public class ASKVisitor extends OpVisitorUnimplemented{

    ASKParallel asks;
    List<Triple> triples = new ArrayList<>();

    public ASKVisitor(Set<String> endpoints) {
        this.asks = new ASKParallel(endpoints); // default filter: only constants subject || object
    }

    public void setDataset(Dataset dataset) {
        this.asks.setDataset(dataset);
    }

    public Map<ImmutablePair<String, Triple>, Boolean> getAsks() {
        return this.asks.getAsks();
    }


    public void visit(Op op) {
        op.visit(this);
        // TODO change triples so when they have the same pattern
        // TODO but the variable name changes, they are considered the same
        this.asks.execute(triples);
    }

    /* ******************************************************* */

    @Override
    public void visit(OpDistinct opDistinct) {
        opDistinct.getSubOp().visit(this);
    }

    @Override
    public void visit(OpProject opProject) {
        opProject.getSubOp().visit(this);
    }

    @Override
    public void visit(OpGraph opGraph) {
        opGraph.getSubOp().visit(this);
    }

    @Override
    public void visit(OpBGP opBGP) {
        this.triples.addAll(opBGP.getPattern().getList());
    }

    @Override
    public void visit(OpQuad opQuad) {
        this.triples.add(opQuad.getQuad().asTriple());
    }

    @Override
    public void visit(OpJoin opJoin) {
        opJoin.getLeft().visit(this);
        opJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpUnion opUnion) {
        opUnion.getLeft().visit(this);
        opUnion.getRight().visit(this);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        opLeftJoin.getLeft().visit(this);
        opLeftJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpConditional opCondition) {
        opCondition.getLeft().visit(this);
        opCondition.getRight().visit(this);
    }

    @Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);
    }

    @Override
    public void visit(OpFilter opFilter) {
        opFilter.getSubOp().visit(this);
    }
}
