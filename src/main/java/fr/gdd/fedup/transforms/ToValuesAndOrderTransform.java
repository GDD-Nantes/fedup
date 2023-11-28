package fr.gdd.fedup.transforms;

import fr.gdd.fedup.asks.ASKVisitor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.table.TableN;
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

    ASKVisitor asks;
    Map<Triple, List<String>> triple2Endpoints = new HashMap<>();
    Map<Triple, Integer> triple2NbEndpoints = new HashMap<>();
    Set<Var> tracker = new HashSet<>();

    Map<OpTable, OpQuad> values2quad = new HashMap<>(); // to avoid adding a filter when we already have a values

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
        this.values2quad = copy.values2quad;
    }

    public void setDataset(Dataset dataset) {
        this.asks.setDataset(dataset);
    }

    public Op transform(Op op) {
        // #1 perform all necessary ASKs
        asks.visit(op);
        // #2 <endpoint, triple> -> boolean to triple -> list<endpoint>
        for (var entry : asks.getAsks().entrySet()) {

            if (!triple2Endpoints.containsKey(entry.getKey().getValue())) {
            // if (!triple2Endpoints.containsKey(entry.getKey().getValue()) && entry.getValue()) {
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
    public Op transform(OpQuad opQuad) {
        return opQuad;
    }

    @Override
    public Op transform(OpSequence opSequence, List<Op> list) {
        if (opSequence.getElements().stream().anyMatch(op -> !(op instanceof OpQuad))) {
            return opSequence;
        }
        List<OpQuad> candidates = new ArrayList<>(opSequence.getElements().stream().map(op -> (OpQuad) op).toList());
        // #1 sort by number of sources; one without sources are implicitly not candidate

        OpSequence sequence = OpSequence.create();

        // #2 rebuild the sequence of operators with values at the right position
        while (!candidates.isEmpty()) {
            candidates.sort((q1, q2) -> {
                Integer q1NbAsks = triple2NbEndpoints.getOrDefault(q1.getQuad().asTriple(), Integer.MAX_VALUE);
                Integer q2NbAsks = triple2NbEndpoints.getOrDefault(q2.getQuad().asTriple(), Integer.MAX_VALUE);
                return Integer.compare(q1NbAsks, q2NbAsks);
            });

            boolean isValues = false;
            OpQuad candidate = getOpQuadWithAlreadySetVariable(candidates, tracker);
            if (Objects.isNull(candidate)) { // no candidate, i.e., cartesian product or first variable to set
                candidate = candidates.getFirst();
                isValues = triple2Endpoints.containsKey(candidate.getQuad().asTriple());
                if (!isValues) { // no ASK can help us
                    candidate = getBestVariableCounting(candidates);
                }
            }

            if (isValues) {
                OpTable values = prepareValues((Var) candidate.getQuad().getGraph(), triple2Endpoints.get(candidate.getQuad().asTriple())); // TODO: does not support named graphs
                values2quad.put(values, candidate);
                sequence.add(values);
            }
            sequence.add(candidate);
            candidates.remove(candidate);

            tracker.addAll(VarUtils.getVars(candidate.getQuad().asTriple()));
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

    @Override
    public Op transform(OpJoin opJoin, Op left, Op right) {
        ToValuesAndOrderTransform rightTransform = new ToValuesAndOrderTransform(this, this.tracker);
        rightTransform.tracker.addAll(OpVars.visibleVars(opJoin.getLeft()));

        // We get the variable set on the left side and inform right side
        return OpJoin.create(Top2BottomTransformer.transform(this, opJoin.getLeft()),
                Top2BottomTransformer.transform(rightTransform, opJoin.getRight()));
    }

    /* ********************************************************************* */

    /**
     * @param candidates The list of triples.
     * @param tracker The variable tracker of set variables.
     * @return A candidate that already has variables set.
     */
    public static OpQuad getOpQuadWithAlreadySetVariable(List<OpQuad> candidates, Set<Var> tracker) {
        var filtered = candidates.stream().filter(q -> VarUtils.getVars(q.getQuad().asTriple()).stream().anyMatch(tracker::contains));
        return filtered.findFirst().orElse(null);
    }

    /**
     * @param candidates The list of triples.
     * @return A quad the number of variables of which is the smallest.
     */
    public static OpQuad getBestVariableCounting(List<OpQuad> candidates) {
        var candidate2NbVars = candidates.stream()
                .map(q -> new ImmutablePair<>(q, VarUtils.getVars(q.getQuad().asTriple()).size()))
                .sorted(Comparator.comparingInt(ImmutablePair::getValue)).map(ImmutablePair::getKey);
        return candidate2NbVars.findFirst().orElse(null);
    }

    /**
     * @param endpoints The set of endpoints
     * @return The VALUES operator comprising the list of sources with a placeholder for the variable name
     */
    public static OpTable prepareValues(Var graph, List<String> endpoints) {
        TableN table = new TableN();
        endpoints.forEach(
                e -> table.addBinding(
                        Binding.builder().add(graph, NodeFactory.createURI(e)).build()
                )
        );
        return OpTable.create(table);
    }
}
