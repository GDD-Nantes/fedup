package fr.gdd.fedup.source.selection.transforms;

import fr.gdd.fedup.source.selection.asks.ASKVisitor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.*;
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
public class ToValuesAndOrderTransform extends TransformUnimplemented {

    private static int PLACEHOLDER_NB = 0;

    ASKVisitor asks;
    Map<Triple, List<String>> triple2Endpoints = new HashMap<>();
    Map<Triple, Integer> triple2NbEndpoints = new HashMap<>();
    Set<Var> tracker = new HashSet<>();

    Map<OpTable, Triple> values2triple = new HashMap<>();

    public ToValuesAndOrderTransform(Set<String> endpoints) {
        this.asks = new ASKVisitor(endpoints);
    }

    /**
     *  Copies everything but the tracker
     */
    public ToValuesAndOrderTransform(ToValuesAndOrderTransform copy, Set<Var> tracker) {
        this.asks = copy.asks;
        this.triple2NbEndpoints = copy.triple2NbEndpoints;
        this.triple2Endpoints = copy.triple2Endpoints;
        this.tracker = new HashSet<>(tracker);
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

        return Top2BottomTransformer.transform(this, op);
    }

    /* ******************************************************************* */

    @Override
    public Op transform(OpBGP opBGP) {
        List<Triple> candidates = opBGP.getPattern().getList();
        // #1 sort by number of sources; one without sources are implicitly not candidate


        OpSequence sequence = OpSequence.create();
        List<Triple> orderedTriples = new ArrayList<>();

        // #2 rebuild the sequence of operators with values at the right position
        while (!candidates.isEmpty()) {
            List<Triple> sortedByNbEndpoints = triple2NbEndpoints.entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .filter(t -> candidates.contains(t))
                    .toList();

            boolean isValues = false;
            Triple candidate = getTripleWithAlreadySetVariable(candidates, tracker);
            if (Objects.isNull(candidate)) { // no candidate, i.e., cartesian product or first variable to set
                candidate = sortedByNbEndpoints.stream().findFirst().orElse(null);
                isValues = Objects.nonNull(candidate);
                if (Objects.isNull(candidate)) { // no ASK can help us
                    candidate = getBestVariableCounting(candidates);
                }
            }

            if (isValues) { // BGP VALUES BGP
                if (!orderedTriples.isEmpty()) { // BGP
                    sequence.add(new OpBGP(BasicPattern.wrap(orderedTriples)));
                    orderedTriples = new ArrayList<>();
                }
                OpTable values = prepareValues(triple2Endpoints.get(candidate)); // VALUES
                sequence.add(values);
                values2triple.put(values, candidate);
            } // BGP

            orderedTriples.add(candidate);
            candidates.remove(candidate);
            tracker.addAll(VarUtils.getVars(candidate));
        }

        if (!orderedTriples.isEmpty()) { // last one
            sequence.add(new OpBGP(BasicPattern.wrap(orderedTriples)));
        }
        return sequence.size() > 1 ? sequence : sequence.get(0);
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        ToValuesAndOrderTransform rightTransform = new ToValuesAndOrderTransform(this, this.tracker);
        rightTransform.tracker.addAll(OpVars.visibleVars(opLeftJoin.getLeft()));

        // We get the variable set on the left side and inform right side
        return OpLeftJoin.create(Top2BottomTransformer.transform(this, opLeftJoin.getLeft()),
                Top2BottomTransformer.transform(rightTransform, opLeftJoin.getRight()),
                opLeftJoin.getExprs());
    }

    @Override
    public Op transform(OpConditional opCond, Op left, Op right) { // same as LeftJoin
        ToValuesAndOrderTransform rightTransform = new ToValuesAndOrderTransform(this, this.tracker);
        rightTransform.tracker.addAll(OpVars.visibleVars(opCond.getLeft()));

        // We get the variable set on the left side and inform right side
        return new OpConditional(Top2BottomTransformer.transform(this, opCond.getLeft()),
                Top2BottomTransformer.transform(rightTransform, opCond.getRight()));
    }

    @Override
    public Op transform(OpFilter opFilter, Op op) {
        return OpFilter.filterBy(opFilter.getExprs(), Top2BottomTransformer.transform(this, opFilter.getSubOp()));
    }

    @Override
    public Op transform(OpUnion opUnion, Op op, Op op1) {
        ToValuesAndOrderTransform rightTransform = new ToValuesAndOrderTransform(this, this.tracker);

        return OpUnion.create(Top2BottomTransformer.transform(this, opUnion.getLeft()),
                Top2BottomTransformer.transform(rightTransform, opUnion.getRight()));
    }

    /* ********************************************************************* */

    /**
     * @param candidates The list of triples.
     * @param tracker The variable tracker of set variables.
     * @return A candidate that already has variables set.
     */
    public static Triple getTripleWithAlreadySetVariable(List<Triple> candidates, Set<Var> tracker) {
        var filtered = candidates.stream().filter(t -> VarUtils.getVars(t).stream().anyMatch(v ->
                tracker.contains(v)));
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
        PLACEHOLDER_NB += 1;
        TableN table = new TableN();
        endpoints.forEach(
                e -> table.addBinding(
                        Binding.builder().add(Var.alloc("placeholder_"+ PLACEHOLDER_NB),
                                NodeFactory.createURI(e)).build()
                )
        );
        return OpTable.create(table);
    }

}
