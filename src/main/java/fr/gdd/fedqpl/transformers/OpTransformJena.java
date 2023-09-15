package fr.gdd.fedqpl.transformers;

import java.util.List;
import java.util.Set;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;

import fr.gdd.fedqpl.operators.FedQPLOpSet;
import fr.gdd.fedqpl.operators.Filter;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;
import fr.gdd.fedqpl.utils.FedQPLUtils;

public class OpTransformJena extends TransformCopy {

    private List<String> endpoints;

    public OpTransformJena(List<String> endpoints) {
        this.endpoints = endpoints;
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() == 1) {
            return Transformer.transform(this, new OpTriple(triples.get(0)));
        } else {
            Op pipeline = OpJoin.create(
                new OpTriple(triples.get(0)),
                new OpTriple(triples.get(1))
            );

            for (int i = 2; i < triples.size(); i++) {
                pipeline = OpJoin.create(pipeline, new OpTriple(triples.get(i)));
            }

            return Transformer.transform(this, pipeline);
        }

    }

    @Override
    public Op transform(OpTriple opTriple) {
        FedQPLOpSet opList = new FedQPLOpSet();
        for (String e : endpoints) {
            Req req = new Req(opTriple.getTriple(), NodeFactory.createURI(e));

            if (!FedQPLUtils.getSolutions(opList).isEmpty())
                opList.add(req);
        }
        return opList;
    }

    @Override
    public Op transform(OpJoin opJoin, Op left, Op right) {
        FedQPLOpSet opList = new FedQPLOpSet();
        // var Phi_1 = (FedQPLOpSet) Transformer.transform(this, opJoin.getLeft());
        // var Phi_2 = (FedQPLOpSet) Transformer.transform(this, opJoin.getRight());

        var Phi_1 = (FedQPLOpSet) Transformer.transform(this, left);
        var Phi_2 = (FedQPLOpSet) Transformer.transform(this, right);

        for (var phi_1 : Phi_1) {
            for (var phi_2 : Phi_2) {
                Mj mj = new Mj(Set.of(phi_1, phi_2));
                if (!FedQPLUtils.getSolutions(mj).isEmpty()) {
                    opList.add(mj);
                }
            }
        }
        return opList;
    }

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        var Phi = (FedQPLOpSet) Transformer.transform(this, subOp);

        FedQPLOpSet opList = new FedQPLOpSet();
        for (var phi : Phi) {
            opList.add(new Filter(opFilter.getExprs(), phi));
        }

        return opList;

    }

    @Override
    public Op transform(OpSlice opSlice, Op subOp) {
        return new Mu((FedQPLOpSet) Transformer.transform(this, subOp));
    }

}
