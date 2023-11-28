package fr.gdd.fedup.asks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecutionBuilder;
import org.apache.jena.query.QueryExecutionDatasetBuilder;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTPBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Perform ASK queries in parallel to check if triple patterns exist
 * on remote endpoints.
 */
public class ASKParallel {

    /**
     * <endpoint, pattern> -> (true: exists; false: does not exist or running)
     */
    ConcurrentHashMap<ImmutablePair<String, Triple>, Boolean> asks = new ConcurrentHashMap<>();
    Set<String> endpoints;
    Map<String, String> new2oldEndpoints = null;
    Predicate<Triple>[] filters;

    /**
     * For debug and testing purposes, the query builder can be changed to something else than
     * HTTP, as long as it `ask()`.
     */
    QueryExecutionBuilder builder = QueryExecutionHTTPBuilder.create();
    Long timeout = Long.MAX_VALUE;
    Dataset dataset;

    public ASKParallel(Set<String> endpoints, Predicate<Triple>... filters) {
        this.endpoints = endpoints;
        if (Objects.nonNull(filters) && filters.length > 0) {
            this.filters = filters;
        } else {
            this.filters = new Predicate[1];
            this.filters[0] = triple -> triple.getSubject().isVariable() && triple.getObject().isURI() ||
                    triple.getSubject().isURI() && triple.getObject().isVariable();
        }
    }

    public ASKParallel setModifierOfEndpoints(Function<String, String> lambda) {
        if (Objects.isNull(lambda) || Objects.nonNull(dataset)) return this;

        this.new2oldEndpoints = endpoints.stream().map(e -> Map.entry(lambda.apply(e), e))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.endpoints = new2oldEndpoints.keySet();

        return this;
    }

    public ASKParallel setTimeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Means local execution.
     * @param dataset The local dataset to perform asks on.
     */
    public ASKParallel setDataset(Dataset dataset) {
        if (Objects.isNull(dataset)) return this;

        if (Objects.nonNull(new2oldEndpoints)) { // nullifies the modifier since it performs on graphs
            this.endpoints = new2oldEndpoints.keySet();
            new2oldEndpoints = null;
        }

        QueryExecutionDatasetBuilder qedb = new QueryExecutionDatasetBuilder();
        qedb.dataset(dataset);
        this.dataset = dataset;
        this.builder = qedb;
        return this;
    }

    public Map<ImmutablePair<String, Triple>, Boolean> getAsks() {
        if (Objects.isNull(new2oldEndpoints)) {
            return asks;
        }
        return this.asks.entrySet().stream().map(e -> Map.entry(new ImmutablePair<>(
                        new2oldEndpoints.get(e.getKey().getLeft()), // modified endpoint names
                        e.getKey().getRight()),
                e.getValue()) ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /* ********************************************************************** */

    public void execute(List<Triple> triples) {
        for (Predicate<Triple> filter : this.filters) {
            triples = triples.stream().filter(filter).toList();
        }

        List<Future<Void>> futures = new ArrayList<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String endpoint : endpoints) { // one per endpoint per triple
                for (Triple triple : triples) {
                    ImmutablePair<String, Triple> id = new ImmutablePair<>(endpoint, triple); // id of the ask
                    if (!this.asks.containsKey(id)) {
                        this.asks.put(id, false);
                        ASKRunnable runnable = new ASKRunnable(this.asks, endpoint, triple, dataset);
                        Future future = executor.submit(runnable);
                        futures.add(future);
                    }

                }
            }
        } // virtual !

        futures.forEach(f -> { // join threads
            try {
                f.get(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Check if the endpoint had the triple pattern when execute was executed.
     * @param endpoint The endpoint URI as String.
     * @param triple The triple pattern.
     * @return True if it existed; false if it timed out, or does not exist.
     */
    public boolean get(String endpoint, Triple triple) {
        ImmutablePair<String, Triple> id = new ImmutablePair<>(endpoint, triple);
        return this.asks.containsKey(id) && this.asks.get(id);
    }

}
