package fr.gdd.fedup.source.selection.transforms;

import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;

import java.util.Objects;
import java.util.Set;

/**
 * Apply transformations that change a query to a source selection query.
 */
public class ToSourceSelectionTransforms extends TransformCopy {

    Set<String> endpoints;
    Dataset dataset;

    public ToSourceSelectionTransforms(Set<String> endpoints, Dataset... datasets) { // default
        this.endpoints = endpoints;
        this.dataset = (Objects.nonNull(datasets) && datasets.length > 0) ? datasets[0] : null;
    }

    public Op transform(Op op) {
        ToValuesAndOrderTransform tv = new ToValuesAndOrderTransform(endpoints);
        if (Objects.nonNull(dataset)) {
            tv.setDataset(dataset); // for testing and debugging purposes
        }

        ToQuadsTransform tq = new ToQuadsTransform();
        ToValuesWithoutPlaceholderTransform tvwpt = new ToValuesWithoutPlaceholderTransform(tq, tv);

        // #1 remove noisy operators
        op = Transformer.transform(new ToRemoveNoiseTransformer(), op);
        // #2 add VALUES and order triple patterns
        Op opValues = tv.transform(op);
        // #3 add graph clauses to triple patterns
        Op opQuads = Transformer.transform(tq, opValues);
        // #4 force back graph clauses in the corresponding VALUES when need be
        Op opQuadsAndValuesWithoutPlaceholders = Transformer.transform(tvwpt, opQuads);
        return opQuadsAndValuesWithoutPlaceholders;
    }

}
