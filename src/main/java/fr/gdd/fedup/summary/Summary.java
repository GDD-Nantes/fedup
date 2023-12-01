package fr.gdd.fedup.summary;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.QueryEngineTDB;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A summary is a dataset along with transformation operations
 * that efficiently represents an initial dataset.
 */
public class Summary {

    private Dataset summary;
    Transform strategy;
    private String remoteURI = null;

    public Summary(Transform strategy) {
        this.strategy = strategy;
        this.summary = TDB2Factory.createDataset();
    }

    public Summary(Transform strategy, Dataset dataset) {
        this.strategy = strategy;
        this.summary = dataset;
    }

    public Summary(Transform strategy, Location location) {
        this.strategy = strategy;
        this.summary = TDB2Factory.connectDataset(location);
    }

    public Summary setRemote(String remoteURI) {
        this.remoteURI = remoteURI;
        this.summary = DatasetFactory.empty();
        return this;
    }

    /**
     * Modifies the summary by adding the quad after being transformation.
     * @param quad The quad to transform and include.
     */
    public void add(Quad quad) {
        if (Objects.nonNull(remoteURI)) { // TODO
            throw new UnsupportedOperationException("Write on remote summary");
        }

        summary.begin(TxnType.WRITE);
        OpQuad toAdd = (OpQuad) strategy.transform(new OpQuad(quad));
        Model model = summary.getNamedModel(toAdd.getQuad().getGraph().getURI());
        model.add(model.asStatement(toAdd.getQuad().asTriple()));
        summary.commit();
        summary.end();
    }

    public Dataset getSummary() {
        return summary;
    }

    public Transform getStrategy() { return strategy; }

    public Op transform(Op query) {
        return Transformer.transform(strategy, query);
    }

    /**
     * @return The set of all graphs of the summary. They represent the possible endpoints
     * reachable through SERVICE queries.
     */
    public Set<String> getGraphs() {
        Set<String> endpoints = new HashSet<>();
        Query getGraphQuery = Objects.isNull(remoteURI) ?
                QueryFactory.create("SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o}}") :
                QueryFactory.create(""+
                    "SELECT DISTINCT ?g WHERE {SERVICE <"+remoteURI+"> {"+
                        "SELECT DISTINCT ?g { GRAPH ?g {?s ?p ?o}}"+
                    "}}");

        boolean inTxn = this.getSummary().isInTransaction() || Objects.nonNull(remoteURI);

        if (!inTxn) this.getSummary().begin(ReadWrite.READ); // TODO less ugly way ?

        try (QueryExecution qe =  QueryExecutionFactory.create(getGraphQuery, getSummary())) {
            ResultSet iterator = qe.execSelect();
            while (iterator.hasNext()) {
                 Binding b = iterator.nextBinding();
                 endpoints.add(b.get(Var.alloc("g")).getURI());
            }
        }
        if (!inTxn) {
            this.getSummary().commit();
            this.getSummary().end();
        }

        return endpoints;
    }
}
