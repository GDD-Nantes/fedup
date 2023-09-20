package fr.gdd.fedup.source.selection.transforms;

import fr.gdd.fedup.source.selection.asks.ASKVisitor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.VarUtils;

import java.util.*;

/**
 * Places a VALUES clause on top of quads that contain a meaningful constant;
 * and reorder BGPs using a variable counting heuristic. When cartesian products
 * arise, redo values then reorder.
 */
public class ToValuesTransform extends TransformCopy {

    ASKVisitor asks;
    Map<Triple, List<String>> triple2Endpoints = new HashMap<>();
    Map<Triple, Integer> triple2NbEndpoints = new HashMap<>();
    VariableUsageTracker tracker = new VariableUsageTracker();

    Map<OpTable, Triple> values2triple = new HashMap<>();

    public ToValuesTransform(Set<String> endpoints) {
        this.asks = new ASKVisitor(endpoints);
    }

    /**
     *  Copies everything but the tracker
     */
    public ToValuesTransform(ToValuesTransform copy, VariableUsageTracker tracker) {
        this.asks = copy.asks;
        this.triple2NbEndpoints = copy.triple2NbEndpoints;
        this.triple2Endpoints = copy.triple2Endpoints;
        this.tracker = tracker;
        this.values2triple = copy.values2triple;
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
        triple2Endpoints.forEach((key, value) -> triple2NbEndpoints.put(key, value.size()));

        return Transformer.transform(this, op);
    }

    /* ******************************************************************* */

    @Override
    public Op transform(OpBGP opBGP) {
        /*List<ImmutablePair<Triple, Integer>> sortedByNbEndpoints = opBGP.getPattern().getList().stream().map(
                triple -> this.triple2NbEndpoints.containsKey(triple) ?
                            new ImmutablePair<>(triple, this.triple2NbEndpoints.get(triple)):
                            new ImmutablePair<>(triple, Integer.MAX_VALUE)
        ).sorted(Comparator.comparingInt(ImmutablePair::getRight)).collect(Collectors.toList());*/
        // #1 sort by number of sources; one without sources are implicitly not candidate
        List<Map.Entry<Triple, Integer>> sortedByNbEndpoints = triple2NbEndpoints.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue)).toList();

        OpSequence sequence = OpSequence.create();
        List<Triple> orderedTriples = new ArrayList<>();
        List<Triple> candidates = opBGP.getPattern().getList();
        while (!candidates.isEmpty()) {
            boolean isValues = false;
            Triple candidate = getTripleWithAlreadySetVariable(candidates, tracker);
            if (Objects.isNull(candidate)) { // no candidate, i.e., cartesian product or first variable to set
                isValues = true;
                candidate = sortedByNbEndpoints.stream().map(Map.Entry::getKey).findFirst().orElse(null);
                if (Objects.isNull(candidate)) { // no ASK can help us
                    candidate = getBestVariableCounting(candidates);
                }
            }

            if (isValues) { // BGP VALUES BGP
                if (!orderedTriples.isEmpty()) { // BGP
                    sequence.add(new OpBGP(BasicPattern.wrap(orderedTriples)));
                }
                OpTable values = prepareValues(triple2Endpoints.get(candidate)); // VALUES
                sequence.add(values);
                values2triple.put(values, candidate);
                orderedTriples = new ArrayList<>();
            } // BGP

            orderedTriples.add(candidate);
            candidates.remove(candidate);
            tracker.increment(VarUtils.getVars(candidate));
        }

        if (!orderedTriples.isEmpty()) { // last one
            sequence.add(new OpBGP(BasicPattern.wrap(orderedTriples)));
        }
        return sequence;
    }


    /**
     * @param candidates The list of triples.
     * @param tracker The variable tracker of set variables.
     * @return A candidate that already has variables set.
     */
    public static Triple getTripleWithAlreadySetVariable(List<Triple> candidates, VariableUsageTracker tracker) {
        var filtered = candidates.stream().filter(t -> VarUtils.getVars(t).stream().anyMatch(v ->
                    tracker.getUsageCount(v) > 0));
        return filtered.findFirst().orElse(null);
    }

    /**
     * @param candidates The list of triples.
     * @return A triple the number of variables of which is the smallest.
     */
    public static Triple getBestVariableCounting(List<Triple> candidates) {
        var candidate2NbVars = candidates.stream()
                .map(t -> new ImmutablePair<>(t, VarUtils.getVars(t).size()))
                .sorted(Comparator.comparingInt(ImmutablePair::getValue)).map(ImmutablePair::getKey);
        return candidate2NbVars.findFirst().orElse(null);
    }

    /**
     * @param endpoints The set of endpoints
     * @return The VALUES operator comprising the list of sources with a placeholder for the variable name
     */
    public static OpTable prepareValues(List<String> endpoints) {
        TableN table = new TableN();
        endpoints.forEach(
                e -> table.addBinding(
                        Binding.builder().add(Var.alloc("placeholder"),
                        NodeFactory.createURI(e)).build()
                )
        );
        return OpTable.create(table);
    }


}
