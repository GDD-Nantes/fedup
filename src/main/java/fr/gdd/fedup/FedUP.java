package fr.gdd.fedup;

import fr.gdd.fedqpl.FedQPL2FedX;
import fr.gdd.fedqpl.FedQPL2SPARQL;
import fr.gdd.fedqpl.SA2FedQPL;
import fr.gdd.fedqpl.groups.*;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.adapters.TupleQueryResult2QueryIterator;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.RemoveSequences;
import fr.gdd.fedup.transforms.ToSourceSelectionTransforms;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.optimize.TransformFilterConjunction;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParserRegistry;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONParserFactory;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The main class of FedUP that build logical query plans for queries over
 * federations of SPARQL endpoints. Therefore, it only takes care of source selection,
 * not federated execution.
 */
public class FedUP {

    private static final Logger log = LoggerFactory.getLogger(FedUP.class);

    // The quotient summary to retrieve possibly relevant sources.
    final Summary summary;
    // The list of endpoints to consider, retrieved from the summary.
    Set<String> endpoints;
    // Modifiers applied to the list of endpoints to adapt to real addresses.
    Function<String, String> modifierOfEndpoints = null;
    // Factorize some operations when possible
    boolean shouldFactorize = false;
    // If we want to execute it on FedX
    FedXRepository fedx = null; // lazy


    // mostly for testing purposes when there are no real endpoints to query.
    Dataset ds4Asks = null;

    public FedUP (Summary summary) {
        this.summary = summary;
        this.endpoints = this.summary.getGraphs();
    }

    /**
     * Mainly for testing purposes.
     * @param summary The summary of the federation graphs.
     * @param ds4Asks The full dataset of the federation graphs (to perform ASKS on it).
     */
    public FedUP (Summary summary, Dataset ds4Asks) {
        this.summary = summary;
        this.ds4Asks = ds4Asks;
        this.endpoints = this.summary.getGraphs();
    }

    /**
     * Mainly for testing purpose and bypass the `getGraph` that may take a long time.
     * @param summary
     * @param endpoints
     */
    public FedUP (Summary summary, Set<String> endpoints) {
        this.summary = summary;
        this.endpoints = endpoints;
    }

    /**
     * Sometimes, the ingested summary does not reflect the current state
     * of endpoints. To alleviate this issue, this function applies a lambda
     * expression to every element of the list of endpoints.
     * @param lambda The lambda function working on a string and producing a string.
     */
    public FedUP modifyEndpoints(Function<String, String> lambda){
        this.modifierOfEndpoints = lambda;
        return this;
    }

    public FedUP shouldFactorize() {
        this.shouldFactorize = true;
        return this;
    }

    public FedUP shouldNotFactorize() {
        this.shouldFactorize = false;
        return this;
    }

    /* ************************************************************** */

    /**
     * @param queryAsString The query to execute on the federation of endpoints.
     * @return A `TupleExpr` that FedX can easily execute to perform the federated query execution.
     */
    public TupleExpr queryToFedX(String queryAsString) {
        log.debug("Parsing the query {}", queryAsString);
        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
        TupleExpr asFedX = queryJenaToFedX(queryAsOp);
        log.info("Built the following query:\n{}", asFedX);
        return asFedX;
    }

    public TupleExpr queryJenaToFedX(Op queryAsOp) {
        Op asFedQPL = queryToFedQPL(queryAsOp, endpoints);
        if (Objects.isNull(asFedQPL)) {return new EmptySet();} // handle error TODO should be elsewhere ?

        // log.debug(asFedQPL.toString()); // cannot print mu and mj
        log.info("Building the FedX SERVICE query…");
        TupleExpr asFedX = ReturningOpVisitorRouter.visit(new FedQPL2FedX(), asFedQPL);
        return asFedX;
    }

    public Pair<TupleExpr, Op> queryJenaToBothFedXAndJena(Op queryAsOp) {
        Op asFedQPL = queryToFedQPL(queryAsOp, endpoints);
        if (Objects.isNull(asFedQPL)) {return new ImmutablePair<>(new EmptySet(), null);}

        log.info("Building the Jena SERVICE query…");
        Op asSPARQL = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), asFedQPL);
        log.info("Building the FedX SERVICE query…");
        TupleExpr asFedX = ReturningOpVisitorRouter.visit(new FedQPL2FedX(), asFedQPL);
        return new ImmutablePair<>(asFedX, asSPARQL);
    }

    /**
     * @param queryAsString The query to execute on the federation of endpoints.
     * @return An `Op` that Apache Jena can easily execute to perform the federated query execution.
     */
    public Op queryToJena(String queryAsString) {
        log.debug("Parsing the query {}", queryAsString);
        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
        return queryJenaToJena(queryAsOp);
    }

    /**
     * @param queryAsOp The initial federated query.
     * @return A SERVICE query that Apache Jena can execute immediately without parsing it again.
     */
    public Op queryJenaToJena(Op queryAsOp) {
        Op asFedQPL = queryToFedQPL(queryAsOp, endpoints);
        log.info("Building the SPARQL SERVICE query…");
        Op asSPARQL = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), asFedQPL);
        log.info("Built the following query:\n{}", asSPARQL);
        return asSPARQL;
    }

    public String query(Op queryAsOp) {
        return this.query(queryAsOp, this.endpoints);
    }

    public String query(String queryAsString) {
        return this.query(queryAsString, this.endpoints);
    }

    /**
     * @param queryAsString The federated query to execute.
     * @param endpoints The set of SPARQL endpoints.
     * @return A SPARQL 1.1 query with SERVICE clauses to query remote endpoints.
     */
    public String query(String queryAsString, Set<String> endpoints) {
        log.debug("Parsing the query {}", queryAsString);
        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
        return this.query(queryAsOp, endpoints);
    }

    public String query(Op queryAsOp, Set<String> endpoints) {
        Op asFedQPL = queryToFedQPL(queryAsOp, endpoints);
        log.info("Building the SPARQL SERVICE query…");
        if (Objects.isNull(asFedQPL)) { return OpAsQuery.asQuery(OpTable.unit()).toString();}
        Op asSPARQL = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), asFedQPL);

        asSPARQL = Transformer.transform(new TransformFilterConjunction(), asSPARQL); // TODO put this in the visitor
        // TODO it splits the exprList, but we want actual split.
        asSPARQL = ReturningOpVisitorRouter.visit(new FilterPushDownVisitor(), asSPARQL);

        String asSERVICE = OpAsQueryMore.asQuery(asSPARQL).toString();

        log.info("Built the following query:\n{}", asSERVICE);
        return asSERVICE;
    }

    public Op queryToFedQPL (Op queryAsOp, Set<String> endpoints) {
        queryAsOp = ReturningOpVisitorRouter.visit(new RemoveSequences(), queryAsOp);
        log.info("Start making ASK queries on {} endpoints…", endpoints.size());
        // TODO use summary as first filter for ASKS
        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(summary.getStrategy(), true, endpoints)
                .setDataset(ds4Asks) // for testing
                .setModifierOfEndpoints(modifierOfEndpoints); // for difference between ingested graph and remote endpoint
        Op ssQueryAsOp = tsst.transform(queryAsOp);

        log.info("Start executing the source selection query…");
        log.debug(ssQueryAsOp.toString());

        // TODO could be processed using a provenance query
        final List<Map<Var, String>> assignments = new ArrayList<>();
        Set<Integer> seen = new TreeSet<>();

        summary.querySummary(ssQueryAsOp).forEach(b -> {
                    // TODO create FedQPL here
                    // TODO but it's much more difficult in presence of OPTIONAL
                    // TODO but could get faster time for first result when things are sure
                    int hashcode = b.toString().hashCode();
                    if (!seen.contains(hashcode)) {
                        seen.add(hashcode);
                        assignments.add(bindingToMap(b));
                    }
                }
        );

        List<Map<Var, String>> assignments2 = assignments;
        // replacing found endpoints by their updated version
        if (Objects.nonNull(this.modifierOfEndpoints)) {
            assignments2 = assignments2.stream()
                    .map(a -> a.entrySet().stream()
                            .map(e -> Map.entry(e.getKey(), modifierOfEndpoints.apply(e.getValue())))
                            .collect(Collectors.toMap(Map.Entry<Var, String>::getKey, Map.Entry<Var, String>::getValue)))
                    .toList();
        }

        // log.info("Removing duplicates and inclusions in logical plan…");
        // assignments2 = removeInclusions(assignments2); // TODO double check if it can be improved
        log.debug("Assignments comprising {} elements:\n{}", assignments2.size(), assignments2.stream().map(Object::toString).collect(Collectors.joining("\n")));

        // TODO in dedicated class
        // TODO built using a CONSTRUCT query
        Dataset assignmentsAsGraph = TDB2Factory.createDataset();
        assignmentsAsGraph.begin(ReadWrite.WRITE);
        Model defaultModel = ModelFactory.createDefaultModel();
        Integer rowNb = 0;
        for (Map<Var, String> assignment : assignments2) {
            rowNb += 1;
            for (Map.Entry<Var, String> var2source : assignment.entrySet()) {
                assignmentsAsGraph.getNamedModel(var2source.getValue()).add(
                        ResourceFactory.createResource(var2source.getKey().getVarName()),
                        ResourceFactory.createProperty("row"),
                        String.valueOf(rowNb));
            }
        }
        assignmentsAsGraph.setDefaultModel(defaultModel);
        assignmentsAsGraph.commit();
        assignmentsAsGraph.end();

        log.info("Building the FedQPL query…");
        Op asFedQPL = SA2FedQPL.build(queryAsOp, tsst.tqt, assignmentsAsGraph);

        log.info("Optimizing the resulting FedQPL plan…");
        FedQPLOptimizer optimizer = new FedQPLOptimizer()
                .register(new FedQPLSimplifyVisitor()) // TODO configurable
                .register(new FedQPLWithExclusiveGroupsVisitor());

        if (shouldFactorize) {
            optimizer.register(new FactorizeUnionsOfReqsVisitor())
                    .register(new FactorizeUnionsOfLeftJoinsVisitor());
        }

        try { // instead of try catch, include the cases in optimizers
            asFedQPL = optimizer.optimize(asFedQPL);
        } catch (NullPointerException e) {
            return null;
        }

        // log.debug("FedUP plan:\n{}", asFedQPL.toString()); // /!\ this could be a lot of logs
        return asFedQPL;
    }


    /* **************************************************************** */

    public FedXRepository getFedX() {
        // lazily create a FedX query executor
        if (Objects.isNull(fedx)) {
            log.info("Initializing FedX executor…");
            fedx = FedXFactory.newFederation()
                    .withConfig(new FedXConfig() // same as FedUP-experiment
                            .withBoundJoinBlockSize(10) // 10+10 or 20+20 ?
                            .withJoinWorkerThreads(10)
                            .withUnionWorkerThreads(10)
                            .withEnforceMaxQueryTime(Integer.MAX_VALUE)
                            .withDebugQueryPlan(false))
                    .withSparqlEndpoints(List.of()).create();
            // for the standalone jar, it seems mandatory to register these
            // result handler beforehand here.
            TupleQueryResultParserRegistry.getInstance().add(new SPARQLResultsXMLParserFactory());
            TupleQueryResultParserRegistry.getInstance().add(new SPARQLResultsJSONParserFactory());
        }
        return fedx;
    }

    public QueryIterator executeWithFedX(TupleExpr queryAsFedX) {
        // then run the query
        log.info("Running the query using FedX…");
        return new TupleQueryResult2QueryIterator(getFedX().getConnection(), queryAsFedX);
    }

    public QueryIterator executeWithJena(Op queryAsJena) {
        QueryEngineMain engine = new QueryEngineMain(queryAsJena, DatasetFactory.empty().asDatasetGraph(), BindingRoot.create(), new Context());
        log.info("Running the query using Jena…");
        return engine.eval(queryAsJena, DatasetFactory.empty().asDatasetGraph(), BindingRoot.create(), new Context());
    }


    /* **************************************************************** */

    /**
     * Convert the binding into a map of [?g -> uri]
     * @param binding The result mappings.
     * @return A map containing the same information as the binding in a map form.
     */
    static Map<Var, String> bindingToMap(Binding binding) {
        Map<Var, String> bindingAsMap = new HashMap<>();
        Iterator<Var> vars = binding.vars();
        while (vars.hasNext()) {
            Var v = vars.next();
            bindingAsMap.put(v, binding.get(v).toString());
        }
        return bindingAsMap;
    }

}
