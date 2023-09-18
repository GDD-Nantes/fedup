package fr.gdd.fedqpl.transformers;

import fr.gdd.fedqpl.operators.*;
import fr.gdd.fedup.source.selection.ToSourceSelectionQueryTransform;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.tdb2.solver.QueryEngineTDB;

import java.util.*;

/**
 * Transform the initial query into a FedQPL expression representing a federated
 * query.
 */
public class TransformJenaFedQPL extends TransformCopy {

    /**
     * Dataset built using an exploration strategy, most often random walks.
     */
    Dataset dataset;
    QueryEngineFactory engineFactory = QueryEngineTDB.getFactory();

    public TransformJenaFedQPL(Dataset dataset) {
        this.dataset = dataset;
    }

    @Override
    public Op transform(OpTriple opTriple) {
        FedQPLOpSet opList = new FedQPLOpSet();

        ToSourceSelectionQueryTransform ssqt = new ToSourceSelectionQueryTransform();
        Op asQuad = Transformer.transform(ssqt, opTriple);

        dataset.begin(ReadWrite.READ);
        Plan plan = engineFactory.create(asQuad, dataset.asDatasetGraph(), BindingRoot.create(), dataset.getContext());

        QueryIterator iterator = plan.iterator();
        Set<String> graphs = new HashSet<>();
        while (iterator.hasNext()) { // iterate over graphs
            Binding b = iterator.next();
            Var g = ssqt.getVar2Triple().keySet().iterator().next(); // one 1 var in ssqt
            if (!graphs.contains(b.get(g).toString())) {
                graphs.add(b.get(g).toString());
                Req req = new Req(opTriple.getTriple(), b.get(g));
                opList.add(req);
            }
        }
        dataset.end();
        return opList;
    }

    @Override
    public Op transform(OpBGP opBGP) {
        FedQPLOpSet opList = new FedQPLOpSet();
        ToSourceSelectionQueryTransform ssqt = new ToSourceSelectionQueryTransform();
        Op asQuads = Transformer.transform(ssqt, opBGP);

        dataset.begin(ReadWrite.READ);
        Plan plan = engineFactory.create(asQuads, dataset.asDatasetGraph(), BindingRoot.create(), dataset.getContext());

        QueryIterator iterator = plan.iterator();
        HashSet<List<String>> graphs = new HashSet<>();
        while (iterator.hasNext()) { // iterate over graphs
            Binding b = iterator.next();

            Mj mj = new Mj();
            List<String> mjGraphs = new ArrayList<>();
            for (Map.Entry<Var, Triple> v2t: ssqt.getVar2Triple().entrySet()) {
                Req req = new Req(v2t.getValue(), b.get(v2t.getKey()));
                mj.addChild(req);
                mjGraphs.add(b.get(v2t.getKey()).toString());
            }
            if (!graphs.contains(mjGraphs)) {
                graphs.add(mjGraphs);
                opList.add(mj);
            }
        }
        dataset.end();
        return opList;
    }

    @Override
    public Op transform(OpJoin opJoin, Op left, Op right) { // AND
        FedQPLOpSet opList = new FedQPLOpSet();
        // var Phi_1 = (FedQPLOpSet) Transformer.transform(this, opJoin.getLeft());
        // var Phi_2 = (FedQPLOpSet) Transformer.transform(this, opJoin.getRight());

        var Phi_1 = (FedQPLOpSet) Transformer.transform(this, left);
        var Phi_2 = (FedQPLOpSet) Transformer.transform(this, right);

        for (var phi_1 : Phi_1) {
            for (var phi_2 : Phi_2) {
                Mj mj = new Mj(Set.of(phi_1, phi_2));
                //if (!FedQPLUtils.getSolutions(mj).isEmpty()) {
                //    opList.add(mj);
                // }
            }
        }
        return opList;
    }

    @Override
    public Op transform(OpUnion opUnion, Op left, Op right) {
        // (TODO) (TODO) (TODO) (TODO) (TODO) (TODO)
        return super.transform(opUnion, left, right);
    }

    @Override
    public Op transform(OpConditional opCond, Op left, Op right) {
        // (TODO) (TODO) (TODO) (TODO) (TODO) (TODO)
        return super.transform(opCond, left, right);
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        // (TODO) (TODO) (TODO) (TODO) (TODO) (TODO)
        return super.transform(opLeftJoin, left, right);
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
    public Op transform(OpProject opProject, Op subOp) { // Top-most Mu that links everything
        return new Mu((FedQPLOpSet) Transformer.transform(this, subOp));
    }

}
