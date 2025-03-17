package fr.gdd.fedup.summary;

import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.Quad2Pattern;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A summary is a dataset along with transformation operations
 * that efficiently represents an initial dataset.
 */
public class Summary {

    private final static Logger log = LoggerFactory.getLogger(Summary.class);

    private Dataset summary;
    Transform strategy;
    private String remoteURI = null;
    private Set<String> graphs = null; // lazy loading
    Pattern patternForGraphs = null;

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
        long start = System.currentTimeMillis();
        this.summary = TDB2Factory.connectDataset(location);
        log.info("Took {} ms to open the summary.", (System.currentTimeMillis() - start));
    }

    public Summary setRemote(String remoteURI) {
        this.remoteURI = remoteURI;
        this.summary = DatasetFactory.empty();
        return this;
    }

    public Summary setPattern(String patternAsString) {
        this.patternForGraphs = Pattern.compile(patternAsString);
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

    public int add(Iterator<Quad> quads) {
        if (Objects.nonNull(remoteURI)) { // TODO
            throw new UnsupportedOperationException("Write on remote summary");
        }

        Set<Quad> summarized = toAdd(quads);

        summary.begin(TxnType.WRITE);
        summarized.forEach(q-> {
            Model model = summary.getNamedModel(q.getGraph().getURI());
            model.add(model.asStatement(q.asTriple()));
        });
        summary.commit();
        summary.end();
        return summarized.size();
    }

    /**
     * @param quads The list of quads to summarize.
     * @return The list of summarized quad.
     */
    public Set<Quad> toAdd(Iterator<Quad> quads) {
        Set<Quad> result = new HashSet<>();
        while (quads.hasNext()) {
            Quad quad = quads.next();
            OpQuad toAdd = (OpQuad) strategy.transform(new OpQuad(quad));
            result.add(toAdd.getQuad());
        }
        return result;
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
        if (Objects.nonNull(this.graphs)) {
            return this.graphs;
        }
        long start = System.currentTimeMillis();
        Op getGraphsQuery = Algebra.compile(QueryFactory.create("SELECT DISTINCT ?g { GRAPH ?g {?s ?p ?o}}"));
        List<Binding> bindings = querySummary(getGraphsQuery);
        log.info("Took {} ms to get graphs.", (System.currentTimeMillis() - start));
        this.graphs = bindings.stream().map(b -> {
                    if (Objects.nonNull(patternForGraphs)) {
                        String graphUriAsString = b.get(Var.alloc("g")).getURI();
                        Matcher matcher = patternForGraphs.matcher(graphUriAsString);
                        if (matcher.matches()) { // return only graphs that match the pattern
                            return b.get(Var.alloc("g")).getURI();
                        } else {
                            return null;
                        }
                    } else { // otherwise return everything
                        return b.get(Var.alloc("g")).getURI();
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        return this.graphs;
    }


    /**
     * @param queryAsOp The `Op` representation of the query to perform on the summary.
     * @return The materialized bindings.
     */
    public List<Binding> querySummary(Op queryAsOp) {
        boolean inTxn = this.getSummary().isInTransaction() || Objects.nonNull(remoteURI);
        if (!inTxn) this.getSummary().begin(ReadWrite.READ);

        // Query query = Objects.isNull(this.remoteURI) ?
        //        OpAsQueryMore.asQuery(queryAsOp) : // otherwise we add a service clause in front
        //        OpAsQueryMore.asQuery(new OpService(NodeFactory.createURI(remoteURI), queryAsOp, true));
        if (Objects.nonNull(remoteURI)) {
            queryAsOp = ReturningOpVisitorRouter.visit(new Quad2Pattern(), queryAsOp);
            // Querying the Virtuoso endpoint with large filter expression leads to error 400
            // so instead, we create a FILTER (?g IN (…))
            queryAsOp = new OpService(NodeFactory.createURI(remoteURI), queryAsOp, false);
        }

        // TODO make sure it does not loop with {@link FedUPServer} and {@link FedUPEngine}
        List<Binding> bindings = new ArrayList<>();
        Plan plan = QueryEngineMain.getFactory().create(queryAsOp,
                getSummary().asDatasetGraph(),
                BindingRoot.create(),
                getSummary().getContext().copy());

        QueryIterator iterator = plan.iterator();

        while (iterator.hasNext()) {
            Binding b = iterator.nextBinding();
            bindings.add(b);
        }

        if (!inTxn) {
            this.getSummary().commit();
            this.getSummary().end();
        }

        return bindings;
    }

}
