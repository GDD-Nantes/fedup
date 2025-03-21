package fr.gdd.fedup.asks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTPBuilder;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Task of a thread that consists in performing an ASK query and register the result
 * in a shared map.
 */
public class ASKRunnable implements Runnable {

    ConcurrentHashMap<ImmutablePair<String, Triple>, Boolean> asks;
    Triple triple;
    String endpoint;
    QueryExecutionBuilder builder;
    Dataset dataset;

    public static Integer RETRY = 5;

    public ASKRunnable(ConcurrentHashMap asks, String endpoint, Triple triple, Dataset dataset) {
        this.asks = asks;
        this.triple = triple;
        this.endpoint = endpoint;
        if (Objects.isNull(dataset)) {
            this.builder = QueryExecutionHTTPBuilder.create();
            ((QueryExecutionHTTPBuilder) this.builder).endpoint(endpoint);
        } else {
            this.builder = QueryExecutionDatasetBuilder.create();
            ((QueryExecutionDatasetBuilder) this.builder).dataset(dataset);
        }
        this.dataset = dataset;
    }

    @Override
    public void run() {
        ImmutablePair<String, Triple> id = new ImmutablePair<>(endpoint, triple);
        boolean response = switch (builder) {
            case QueryExecutionHTTPBuilder b -> { // remote
                int retry = RETRY;
                Query query = OpAsQuery.asQuery(new OpTriple(triple));
                // Query query = OpAsQuery.asQuery(new OpSlice(new OpTriple(triple), 0, 1));
                query.setQueryAskType();
                while (retry > 0) {
                    try {
                        // var results = b.query(query).timeout(5, TimeUnit.SECONDS).select();
                        // yield results.hasNext();
                        yield b.query(query).timeout(5000, TimeUnit.MILLISECONDS).ask();
                    } catch (QueryException e) {
                        retry -= 1;
                    }
                }
                yield false; // failed to call the remote server, default to false
            }
            case QueryExecutionDatasetBuilder b -> { // local
                Node graph = NodeFactory.createURI(endpoint);
                Query query = OpAsQuery.asQuery(new OpGraph(graph, new OpTriple(triple)));
                query.setQueryAskType();
                dataset.begin(ReadWrite.READ);
                boolean r = b.query(query).ask(); // dataset must be in read txn
                dataset.commit();
                dataset.end();
                yield r;
            }
            default -> throw new UnsupportedOperationException();
        };
        this.asks.replace(id, response);
    }

}
