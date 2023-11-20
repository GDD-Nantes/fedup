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

/**
 * Apply transformations that change a query to a source selection query.
 */
public class ToSourceSelectionTransforms {

    Transform summarizer;
    Set<String> endpoints;
    Dataset dataset;

    public boolean asDistinctGraphs;

    public ToQuadsTransform tqt;

    public ToSourceSelectionTransforms(Transform summarizer, boolean asDistinctGraph, Set<String> endpoints, Dataset... datasets) { // default
        this.summarizer = summarizer;
        this.asDistinctGraphs = asDistinctGraph;
        this.endpoints = endpoints;
        this.dataset = (Objects.nonNull(datasets) && datasets.length > 0) ? datasets[0] : null;
    }

    public Op transform(Op op) {
        ToValuesAndOrderTransform tv = new ToValuesAndOrderTransform(endpoints);
        if (Objects.nonNull(dataset)) {
            tv.setDataset(dataset); // for testing and debugging purposes
        }

        tqt = new ToQuadsTransform();
        // ToValuesWithoutPlaceholderTransform tvwpt = new ToValuesWithoutPlaceholderTransform(tq, tv);

        // #1 remove noisy operators
        op = Transformer.transform(new ToRemoveNoiseTransformer(), op);
        // #3 add graph clauses to triple patterns
        op = Transformer.transform(tqt, op);
        // #2 add VALUES and order triple patterns
        op = tv.transform(op);

        AddFilterForAskedGraphs affag = new AddFilterForAskedGraphs(tv);

        if (summarizer instanceof ModuloOnSuffix) {
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
