package fr.gdd.fedup.startup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;
import fr.univnantes.gdd.fedup.sourceselection.SourceAssignments;

public class FedUPQueryExecutor {
    
    private SailRepositoryConnection connection; // is connection thread safe??

    public FedUPQueryExecutor(SailRepositoryConnection connection) {
        this.connection = connection;
    }

    public void execute(String queryString, SourceAssignments assignments, Spy spy) throws Exception {
        int numSubQueries = assignments.getAssignments().size(); 

        ExecutorService executor = Executors.newFixedThreadPool(Math.max(Math.min(numSubQueries, 8), 1));
        ResultsManager resultsManager = new ResultsManager(numSubQueries, Utils.getLimit(queryString));
        List<Future<?>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < numSubQueries; i++) {
                Future<?> future = executor.submit(() -> {
                    TupleQuery query = this.connection.prepareTupleQuery(queryString);
                    TupleQueryResult results = query.evaluate();
                    while (results.hasNext()) {
                        if (resultsManager.addSolution(results.next())) {
                            break;
                        }
                    }
                    resultsManager.notifyComplete();
                });
                futures.add(future);
            }
            resultsManager.waitForResults();
            for (Future<?> future: futures) {
                future.cancel(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
        long endTime = System.currentTimeMillis();
        
        // TODO: Returns solutions
        spy.solutions = resultsManager.getRawSolutions();
        spy.numSolutions += resultsManager.getSolutions().size();
        spy.executionTime = endTime - startTime;
    }

    private class ResultsManager {

        private int remainingProducers;
        private long limit;
        private List<Integer> solutions;
        private List<String> solutions_raw;

        public ResultsManager(int numProducers, long limit) {
            this.remainingProducers = numProducers;
            this.limit = limit;
            this.solutions = new ArrayList<>();
            this.solutions_raw = new ArrayList<>();
        }

        public synchronized void waitForResults() {
            while (this.solutions.size() < this.limit && this.remainingProducers > 0) {
                try {
                    wait();
                } catch (InterruptedException e) { }
            }
        }

        public synchronized boolean addSolution(BindingSet solution) {
            this.solutions.add(solution.toString().hashCode());
            this.solutions_raw.add(solution.toString());
            notifyAll();
            return this.solutions.size() >= this.limit;
        }

        public synchronized void notifyComplete() {
            this.remainingProducers -= 1;
            notifyAll();
        }

        public List<Integer> getSolutions() {
            return this.solutions;
        }

        public List<String> getRawSolutions() {
            return this.solutions_raw;
        }
    }
}
