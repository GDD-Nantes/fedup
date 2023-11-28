package fr.gdd.fedup.transforms;

import fr.gdd.fedup.summary.ModuloOnSuffix;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Apply transformations that change a query to a source selection query.
 */
public class ToSourceSelectionTransforms {

    Transform summarizer;
    Set<String> endpoints;
    Dataset dataset = null;
    Function<String, String> modifierOfEndpoints = null;

    public boolean asDistinctGraphs;

    public ToQuadsTransform tqt;

    public ToSourceSelectionTransforms(Transform summarizer, boolean asDistinctGraph, Set<String> endpoints) { // default
        this.summarizer = summarizer;
        this.asDistinctGraphs = asDistinctGraph;
        this.endpoints = endpoints;
    }

    public ToSourceSelectionTransforms setDataset(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }

    public ToSourceSelectionTransforms setModifierOfEndpoints(Function<String, String> modifierOfEndpoints) {
        this.modifierOfEndpoints = modifierOfEndpoints;
        return this;
    }

    public Op transform(Op op) {
        // #0 performs ASKs
        ToValuesAndOrderTransform tv = new ToValuesAndOrderTransform(endpoints)
                .setDataset(dataset)
                .setModifierOfEndpoints(modifierOfEndpoints);

        // #1 remove noisy operators
        op = Transformer.transform(new ToRemoveNoiseTransformer(), op);
        // #3 add graph clauses to triple patterns
        tqt = new ToQuadsTransform();
        op = Transformer.transform(tqt, op);
        // #2 add VALUES and order triple patterns
        op = tv.transform(op);

        AddFilterForAskedGraphs affag = new AddFilterForAskedGraphs(tv);

        if (summarizer instanceof ModuloOnSuffix) { // TODO fix ugliness
            ((ModuloOnSuffix) summarizer).setAffag(affag);
        }

        op = Transformer.transform(affag, op);

        op = Transformer.transform(summarizer, op);

        // #5 wraps it in a projection distinct graphs
        if (asDistinctGraphs) {
            List<Var> graphs = tqt.var2quad.keySet().stream().toList();
            OpProject opProject = new OpProject(op, graphs);
            return new OpDistinct(opProject);
        } else {
            return op;
        }
    }

}
