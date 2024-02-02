package fr.gdd.fedup.fuseki;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.adapters.TupleQueryResult2QueryIterator;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.RemoveGraphsTransform;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.eclipse.rdf4j.federated.optimizer.OptimizerUtil;
import org.eclipse.rdf4j.federated.repository.FedXRepositoryConnection;
import org.eclipse.rdf4j.federated.structures.FedXTupleQuery;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;

public class FedUPEngine extends QueryEngineTDB {

    protected FedUPEngine(Op op, DatasetGraphTDB dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected FedUPEngine(Query query, DatasetGraphTDB dataset, Binding input, Context context) {
        super(query, dataset, input, context);
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(factory);
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        op = Transformer.transform(new RemoveGraphsTransform(), op);

        // TODO fedup factory builds this in main
        FedUP fedup = new FedUP(new Summary(new ModuloOnSuffix(1), DatasetImpl.wrap(dsg)))
                .shouldNotFactorize()
                .modifyEndpoints(e-> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0,e.length()-1)));

        if (context.get(FedUPConstants.EXECUTION_ENGINE).equals(FedUPConstants.FEDX)) {
            if (context.isTrue(FedUPConstants.EXPORT_PLANS)) {
                Pair<TupleExpr, Op> query4both = fedup.queryJenaToBothFedXAndJena(op);
                context.set(FedUPConstants.EXPORTED, query4both.getRight());
                return fedup.executeWithFedX(query4both.getLeft());

            } else {
                TupleExpr query4FedX = fedup.queryJenaToFedX(op);
                return fedup.executeWithFedX(query4FedX);
            }
        }

        // default engine is Jena:
        Op serviceQueryAsOp = fedup.queryJenaToJena(op);
        context.set(FedUPConstants.EXPORTED, serviceQueryAsOp); // it costs barely nothing
        return super.eval(serviceQueryAsOp, DatasetFactory.empty().asDatasetGraph(), BindingRoot.create(), new Context());
    }

    /* ******************** Factory ********************** */
    public static QueryEngineFactory factory = new FedUPEngineFactory();

    public static class FedUPEngineFactory extends QueryEngineFactoryTDB {

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding input, Context context) {
            QueryEngineBase engine = new FedUPEngine(query, dsgToQuery(dataset), input, context);
            return engine.getPlan();
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            QueryEngineBase engine = new FedUPEngine(op, dsgToQuery(dataset), binding, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return !dataset.isEmpty() && super.accept(op, dataset, context);
        }

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return onlySELECT(query) &&
                    !dataset.isEmpty() &&
                    super.accept(query, dataset, context);
        }

        private static boolean onlySELECT(Query query) {
            return query.isSelectType();
        }
    }
}
