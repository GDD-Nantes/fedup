package fr.gdd.fedup.summary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Vars;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Adds Graph clauses to retrieves the necessary data to perform
 * source selection and create FedQPL expression.
 * TODO change the name to a more specific one
 */
public class QueryTransformer extends TransformCopy {

    Integer nbGraphs = 0;
    Boolean selectAll = false;

    public QueryTransformer(Boolean selectAll) {
        this.selectAll = selectAll; // (TODO) maybe select only graphs
    }

    @Override
    public Op transform(OpTriple opTriple) {
        nbGraphs += 1;
        Node g = NodeFactory.createVariable("g" + nbGraphs);
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
        Op transformedSubOp = Transformer.transform(this, subOp);
        return new OpProject(transformedSubOp, new ArrayList<>(OpVars.visibleVars(transformedSubOp)));
    }

    @Override
    public Op transform(OpSlice opSlice, Op subOp) {
        return Transformer.transform(this, subOp);
    }
}
