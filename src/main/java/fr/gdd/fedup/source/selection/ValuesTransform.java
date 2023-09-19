package fr.gdd.fedup.source.selection;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Places a VALUES clause on top of quads that contain a meaningful constant;
 * and reorder BGPs using a variable counting heuristic.
 */
public class ValuesTransform extends TransformCopy {

    ASKVisitor asks;
    Map<Triple, List<String>> triple2Endpoints = new HashMap<>();
    Map<Triple, Integer> triple2NbEndpoints = new HashMap<>();
    VariableUsageTracker tracker;

    public ValuesTransform(Set<String> endpoints) {
        this.asks = new ASKVisitor(endpoints);
        this.tracker = new VariableUsageTracker();
    }

    /**
     *  Copies everything but the tracker
     */
    public ValuesTransform(ValuesTransform copy, VariableUsageTracker tracker) {
        this.asks = copy.asks;
        this.triple2NbEndpoints = copy.triple2NbEndpoints;
        this.triple2Endpoints = copy.triple2Endpoints;
        this.tracker = tracker;
    }

    public void setDataset(Dataset dataset) {
        this.asks.setDataset(dataset);
    }

    public Op transform(Op op) {
        // #1 perform all necessary ASKs
        asks.visit(op);
        // #2 <endpoint, triple> -> boolean to triple -> list<endpoint>
        for (var entry : asks.getAsks().entrySet()) {
            if (!triple2Endpoints.containsKey(entry.getKey().getValue()) && entry.getValue()) {
                triple2Endpoints.put(entry.getKey().getValue(), new ArrayList<>());
            }
            if (entry.getValue()) {
                triple2Endpoints.get(entry.getKey().getValue()).add(entry.getKey().getKey());
            }
        }
        triple2Endpoints.entrySet().forEach(p -> triple2NbEndpoints.put(p.getKey(), p.getValue().size()));

        return Transformer.transform(this, op);
    }

    /* ******************************************************************* */

    @Override
    public Op transform(OpBGP opBGP) {
        List<ImmutablePair<Triple, Integer>> sortedByNbEndpoints = opBGP.getPattern().getList().stream().map(
                triple -> this.triple2NbEndpoints.containsKey(triple) ?
                            new ImmutablePair<>(triple, this.triple2NbEndpoints.get(triple)):
                            new ImmutablePair<>(triple, Integer.MAX_VALUE)
        ).sorted(Comparator.comparingInt(ImmutablePair::getRight)).collect(Collectors.toList());



        return super.transform(opBGP);
    }
}
