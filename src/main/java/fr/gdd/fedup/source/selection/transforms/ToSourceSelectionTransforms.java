package fr.gdd.fedup.source.selection.transforms;

import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;

import java.util.Objects;
import java.util.Set;

/**
 * Apply transformations that change a query to a source selection query.
 */
public class ToSourceSelectionTransforms {

    Set<String> endpoints;
    Dataset dataset;

    public ToSourceSelectionTransforms(Set<String> endpoints, Dataset... datasets) { // default
        this.endpoints = endpoints;
        this.dataset = (Objects.nonNull(datasets) && datasets.length > 0) ? datasets[0] : null;
    }

    public Op transform(Op op) {
        ToValuesTransform tv = new ToValuesTransform(endpoints);
        if (Objects.nonNull(dataset)) {
            tv.setDataset(dataset); // for testing and debugging purposes
        }
        ToQuadsTransform tq = new ToQuadsTransform();
        ToValuesWithoutPlaceholderTransform tvwpt = new ToValuesWithoutPlaceholderTransform(tq, tv);
        Op opValues = tv.transform(op);
        Op opQuads = Transformer.transform(tq, opValues);
        Op opQuadsAndValuesWithoutPlaceholders = Top2BottomTransformer.transform(tvwpt, opQuads);
        return opQuadsAndValuesWithoutPlaceholders;
    }

}
