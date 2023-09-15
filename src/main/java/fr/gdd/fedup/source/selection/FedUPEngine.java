package fr.gdd.fedup.source.selection;

import fr.gdd.fedup.summary.strategies.ModuloOnSuffix;
import fr.gdd.raw.QueryEngineRAW;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the machinery to execute a query in a federated context, i.e.,
 * with multiple -- possibly remote -- endpoints hosting the data to retrieve.
 */
public class FedUPEngine {

    Logger log = LoggerFactory.getLogger(FedUPEngine.class);

    public void FedUPEngine() {
        // Iterator
        // FedQPL = new FedUPIterator(
        // SourceSelectionIterator)).next()

        // Query + Summary (transformer) -> SSQuery
        // SSQ -> random Walks
        // random walks -> fedqpl
        // fedqpl -> sparql
        // sparql -> mappings

    }

    public QueryIterator executeAsFederatedQuery(String queryAsString, DatasetGraph dataset, Binding input, Context context) {
        return executeAsFederatedQuery(QueryFactory.create(queryAsString), dataset, input, context);
    }

    // Same signature as Jena's `QueryEngineMain`
    public QueryIterator executeAsFederatedQuery(Query query, DatasetGraph dataset, Binding input, Context context) {
        return executeAsFederatedQuery(Algebra.compile(query), dataset, input, context);
    }

    public QueryIterator executeAsFederatedQuery(Op op, DatasetGraph dataset, Binding input, Context context) {
        // #1 create a source selection query using the configured summary
        // (TODO) configurable summary strategy in dataset.getContext() or context
        // QueryExecutionFactory.create()
        Op sourceSelectionQuery = Transformer.transform(new ToSourceSelectionQueryTransform(), op);
        sourceSelectionQuery = Transformer.transform(new ModuloOnSuffix(1), sourceSelectionQuery); // (TODO) change here
        log.debug("Source selection query:\n" + sourceSelectionQuery.toString());


        // #2 perform random walks to feed to our plan generator
        Plan rawPlan = QueryEngineRAW.factory.create(sourceSelectionQuery, dataset, input, context);
        return rawPlan.iterator();

        /*
        // #2 comes from {@link QueryEngineBase}
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg,
                new OpExecutorRandom.OpExecutorRandomFactory(context));
        /*
        if (execCxt.getContext().isUndef(SageConstants.input)) { // <=> setIfUndef
            // (TODO) improve , should this be here? should this have Sage ref' removed ?
            long limit = execCxt.getContext().getLong(SageConstants.limit, Long.MAX_VALUE);
            // always have a timeout otherwise could be infinite looping. Here arbitrarily set to 60s.
            long timeout = execCxt.getContext().getLong(SageConstants.timeout, 60000);
            SageInput<?> sageInput = new SageInput<>().setLimit(limit).setTimeout(timeout);

            execCxt.getContext().set(SageConstants.input, sageInput);
        }

        // #3 inbetween we add our home-made counter iterator :)
        RandomCounterIter counterIter = new RandomCounterIter(op, input, execCxt);

        // Wrap with something to check for closed iterators.
        QueryIterator qIter = QueryIteratorCheck.check(counterIter, execCxt) ;
        // Need call back.
        if ( context.isTrue(ARQ.enableExecutionTimeLogging) )
            qIter = QueryIteratorTiming.time(qIter) ;
        return qIter ; */
    }

}
