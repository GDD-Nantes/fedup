package fr.gdd.fedup.source.selection;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionBuilder;
import org.apache.jena.query.QueryExecutionDatasetBuilder;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Retrieves a set of queries to execute and execute them, possibly parallel.
 */
public class Op2BindingsIterator implements QueryIterator {

    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    FedQPL2SparqlIterator ops;
    ConcurrentLinkedQueue<Binding> bindingsPool = new ConcurrentLinkedQueue<>();

    public Op2BindingsIterator(FedQPL2SparqlIterator ops) {
        this.ops = ops;
    }

    @Override
    public boolean hasNext() {
        while (ops.hasNext() && bindingsPool.isEmpty()) {
            Op op = ops.next();
            executor.submit(() -> {
                // maybe temporary rely on FedX ?
                // TODO TODO TODO
                //QueryExecutionBuilder builder = new QueryExecutionDatasetBuilder();
                // builder.
            });
            // TODO
        }
        return !bindingsPool.isEmpty();
    }

    @Override
    public Binding next() {
        return bindingsPool.poll();
    }

    /* ******************************************************************* */

    @Override
    public void cancel() {
        executor.shutdown(); // close all threads
    }

    @Override
    public void close() {
        executor.shutdown(); // close all threads
    }

    @Override
    public void output(IndentedWriter indentedWriter, SerializationContext serializationContext) {
        // TODO TODO TODO
    }

    @Override
    public String toString(PrefixMapping prefixMapping) {
        // TODO TODO TODO
        return null;
    }

    @Override
    public void output(IndentedWriter indentedWriter) {
        // TODO TODO TODO
    }

    @Override
    public Binding nextBinding() { // alias
        return this.next();
    }
}
