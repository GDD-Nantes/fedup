package fr.gdd.fedup;

import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.RemoveGraphsTransform;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCloseable;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;

import java.util.List;
import java.util.Set;

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
        System.out.println("meow?");
        op = Transformer.transform(new RemoveGraphsTransform(), op);

        FedUP fedup = new FedUP(new Summary( new ModuloOnSuffix(1), DatasetImpl.wrap(dsg)));

        String serviceQuery = fedup.query(op, Set.of("a")); // TODO read graphs from summary

        Op serviceQueryAsOp = Algebra.parse(serviceQuery);

        // TODO this calls eval, need to find a way to create a query iterator from Op;
        // QueryEngineBase qeb = new Quer
        return super.eval(op, DatasetFactory.empty().asDatasetGraph(), BindingRoot.create(), context);
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
