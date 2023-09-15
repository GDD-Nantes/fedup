package fr.gdd.fedup.source.selection;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.raw.QueryEngineRAW;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.tdb2.TDB2Factory;

import java.util.Iterator;
import java.util.Objects;

/**
 * Transform a set of random walks into a FedQPL expression.
 */
public class RWsToFedQPLIterator implements Iterator<FedQPLOperator> {

    Op sourceSelectionQuery;
    Dataset randomWalksTarget;

    Dataset virtualFederation;

    QueryIterator rawIterator;

    Long start;
    Long end;

    public RWsToFedQPLIterator(Op sourceSelectionQuery, Dataset randomWalksTarget) {
        this.virtualFederation = TDB2Factory.createDataset();
        this.sourceSelectionQuery = sourceSelectionQuery;
        this.randomWalksTarget = randomWalksTarget;
    }

    /**
     * Set a stopping condition. Otherwise, could be infinitely running.
     * @param timeout Time after which the iterator should stop checking for new solutions.
     */
    public void setTimeout(Long timeout) {
        this.start = System.currentTimeMillis();
        this.end = start + timeout;
    }

    @Override
    public boolean hasNext() {
        Plan rawPlan = QueryEngineRAW.factory.create(sourceSelectionQuery,
                randomWalksTarget.asDatasetGraph(),
                BindingRoot.create(),
                randomWalksTarget.getContext());

        rawIterator = rawPlan.iterator();

        return (Objects.isNull(end) || System.currentTimeMillis() >= end) &&
                rawIterator.hasNext();
    }

    @Override
    public FedQPLOperator next() {
        // #1 perform the random walks
        virtualFederation.begin(ReadWrite.WRITE);
        while (rawIterator.hasNext()) {
            // #2 create a dataset (virtual federation) using these random walks
            Binding b = rawIterator.next();
            QuadBuilderFromBindingAndPlan rebuild = new QuadBuilderFromBindingAndPlan(b);
            sourceSelectionQuery.visit(rebuild);

            // TODO (or a simpler (efficienter) version where we only need to know graph per TP)
            for (Quad q : rebuild.getQuads()) {
                virtualFederation.asDatasetGraph().add(q);
            }
        }
        virtualFederation.commit();
        virtualFederation.end();
        // #3 TODO create a FedQPL plan
        return null;
    }
}
