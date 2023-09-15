package fr.gdd.fedqpl.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Req;

public class FedQPLUtils {
    public static List<Binding> getSolutions(FedQPLOperator op) {
        if (op instanceof Req) {
            Req opReq = (Req) op;

            Triple triple = opReq.getTriple();
            String source = opReq.getSource().getURI();

            String[] endpoint_infos = source.split("\\?");
            String service = endpoint_infos[0];
            String graph = endpoint_infos[1].split("=")[1];

            Query query = OpAsQuery.asQuery(new OpTriple(triple));
            query.addGraphURI(graph);
            query.setQuerySelectType();

            // ElementGroup body = new ElementGroup();
            // body.addElement(new ElementService(service, query.getQueryPattern()));
            // query.setQueryPattern(body);

            ResultSet iterator = QueryExecutionHTTP.service(service, query).execSelect();

            // Dataset dataset =
            // TDB2Factory.connectDataset(config.getProperty("fedup.summary"));
            // dataset.begin(TxnType.READ);

            // QC.setFactory(dataset.getContext(), new
            // OpExecutorRandom.OpExecutorRandomFactory(ARQ.getContext()));
            // QueryEngineFactory factory = QueryEngineRandom.factory;

            // Context context = dataset.getContext().copy();
            // context.set(SageConstants.timeout, 0);

            // Plan plan = factory.create(query, dataset.asDatasetGraph(),
            // BindingRoot.create(), context);
            // QueryIterator iterator = plan.iterator();

            List<Binding> results = new ArrayList<>();
            while (iterator.hasNext()) {
                Binding binding = iterator.nextBinding();
                results.add(binding);
            }

            return results;
        }
        return null;
    }

}
