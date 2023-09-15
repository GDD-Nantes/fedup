package fr.gdd.fedup.source.selection;

import fr.gdd.raw.io.OpVisitorUnimplemented;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Creates quads from bindings by visiting the plan.
 */
public class QuadBuilderFromBindingAndPlan extends OpVisitorUnimplemented {

    Binding binding;
    List<Quad> quads = new ArrayList<>();

    public QuadBuilderFromBindingAndPlan(Binding binding) {
        this.binding = binding;
    }

    public List<Quad> getQuads() {
        return quads;
    }

    @Override
    public void visit(OpQuad opQuad) {
        Node g = !opQuad.getQuad().getGraph().isVariable() ? opQuad.getQuad().getGraph():
                opQuad.getQuad().getGraph().isVariable() && binding.contains(opQuad.getQuad().getGraph().getName()) ?
                        binding.get(opQuad.getQuad().getGraph().getName()):
                        null;
        Node s = !opQuad.getQuad().getSubject().isVariable() ? opQuad.getQuad().getSubject():
                opQuad.getQuad().getSubject().isVariable() && binding.contains(opQuad.getQuad().getSubject().getName()) ?
                        binding.get(opQuad.getQuad().getSubject().getName()):
                        null;
        Node p = !opQuad.getQuad().getPredicate().isVariable() ? opQuad.getQuad().getPredicate():
                opQuad.getQuad().getPredicate().isVariable() && binding.contains(opQuad.getQuad().getPredicate().getName()) ?
                        binding.get(opQuad.getQuad().getPredicate().getName()):
                        null;
        Node o = !opQuad.getQuad().getObject().isVariable() ? opQuad.getQuad().getObject():
                opQuad.getQuad().getObject().isVariable() && binding.contains(opQuad.getQuad().getObject().getName()) ?
                        binding.get(opQuad.getQuad().getObject().getName()):
                        null;
        if (Objects.nonNull(g) && Objects.nonNull(s) && Objects.nonNull(p) && Objects.nonNull(o)) {
            quads.add(new Quad(g, s, p, o));
        }
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
    public void visit(OpProject opProject) {
        opProject.getSubOp().visit(this);
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
