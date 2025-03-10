package fr.gdd.fedup.fuseki;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.RemoveGraphsTransform;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.function.Function;

public class FedUPEngine extends QueryEngineBase {

    protected FedUPEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected FedUPEngine(Query query, DatasetGraph dataset, Binding input, Context context) {
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
        Summary summary = context.get(FedUPConstants.SUMMARY);

        op = Transformer.transform(new RemoveGraphsTransform(), op);

        FedUP fedup = new FedUP(summary).shouldNotFactorize(); // TODO factorize outside

        if (context.isDefined(FedUPConstants.MODIFY_ENDPOINTS)) {
            Function<String, String> lambda = context.get(FedUPConstants.MODIFY_ENDPOINTS);
            fedup.modifyEndpoints(lambda);
        } else {
            // we keep the default behavior so it does not break anything
            fedup.modifyEndpoints(e -> "http://localhost:5555/sparql?default-graph-uri=" + (e.substring(0, e.length() - 1)));
        }

        return switch (context.getAsString(FedUPConstants.EXECUTION_ENGINE)) {
            case "FedX", "fedx" -> {
                if (context.isTrue(FedUPConstants.EXPORT_PLANS)) {
                    Pair<TupleExpr, Op> query4both = fedup.queryJenaToBothFedXAndJena(op);
                    context.set(FedUPConstants.EXPORTED, query4both.getRight());
                    yield fedup.executeWithFedX(query4both.getLeft());
                } else {
                    TupleExpr query4FedX = fedup.queryJenaToFedX(op);
                    yield fedup.executeWithFedX(query4FedX);
                }
            }
            default -> { // default is Jena
                Op serviceQueryAsOp = fedup.queryJenaToJena(op);
                if (context.isTrue(FedUPConstants.EXPORT_PLANS)) {
                    // we already have it, but it could be costly to write it as string
                    context.set(FedUPConstants.EXPORTED, serviceQueryAsOp);
                }
                yield fedup.executeWithJena(serviceQueryAsOp);
            }
        };
    }

    /* ******************** Factory ********************** */
    public static QueryEngineFactory factory = new FedUPEngineFactory();

    public static class FedUPEngineFactory implements QueryEngineFactory {

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding input, Context context) {
            QueryEngineBase engine = new FedUPEngine(query, dataset, input, context);
            return engine.getPlan();
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            QueryEngineBase engine = new FedUPEngine(op, dataset, binding, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return onlySELECT(OpAsQuery.asQuery(op));
        }

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return onlySELECT(query);
        }

        private static boolean onlySELECT(Query query) {
            return query.isSelectType();
        }
    }
}
