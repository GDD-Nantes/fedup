package fr.gdd.fedup;

import fr.gdd.fedqpl.FedQPL2FedX;
import fr.gdd.fedqpl.FedQPL2SPARQL;
import fr.gdd.fedqpl.SA2FedQPL;
import fr.gdd.fedqpl.groups.FedQPLSimplifyVisitor;
import fr.gdd.fedqpl.groups.FedQPLWithExclusiveGroupsVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.ToSourceSelectionTransforms;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQueryMore;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.util.NodeIsomorphismMap;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
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
    Set<String> endpoints;
    Function<String, String> modifierOfEndpoints = null;

    Dataset ds4Asks = null; // mostly for testing purposes

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
     * Sometimes, the ingested summary does not reflect the current state
     * of endpoints. To alleviate this issue, this function applies a lambda
     * expression to every element of the list of endpoints.
     * @param lambda The lambda function working on a string and producing a string.
     */
    public FedUP modifyEndpoints(Function<String, String> lambda){
        this.modifierOfEndpoints = lambda;
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
        Op asFedQPL = queryToFedQPL(queryAsOp, endpoints);
        log.debug(asFedQPL.toString());
        log.info("Building the FedX SERVICE query…");
        TupleExpr asFedX = ReturningOpVisitorRouter.visit(new FedQPL2FedX(), asFedQPL);
        log.info("Built the following query:\n{}", asFedX);
        return asFedX;
    }

    /**
     * @param queryAsString The query to execute on the federation of endpoints.
     * @return An `Op` that Apache Jena can easily execute to perform the federated query execution.
     */
    public Op queryToJena(String queryAsString) {
        log.debug("Parsing the query {}", queryAsString);
        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
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
        Op asSPARQL = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), asFedQPL);
        String asSERVICE = OpAsQueryMore.asQuery(asSPARQL).toString();

        log.info("Built the following query:\n{}", asSERVICE);
        return asSERVICE;
    }

    public Op queryToFedQPL (Op queryAsOp, Set<String> endpoints) {
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
            }}
        );

        List<Map<Var, String>> assignments2 = assignments;
        // replacing found endpoints by their updated version
        if (Objects.nonNull(this.modifierOfEndpoints)) {
            assignments2 = assignments2.stream()
                    .map(a -> a.entrySet().stream()
                            .map(e -> Map.entry(e.getKey(),modifierOfEndpoints.apply(e.getValue())))
                            .collect(Collectors.toMap(Map.Entry<Var, String>::getKey, Map.Entry<Var, String>::getValue)))
                    .toList();
        }

        log.info("Removing duplicates and inclusions in logical plan…");
        assignments2 = removeInclusions(assignments2); // TODO double check if it can be improved
        log.debug("Assignments comprising {} elements:\n{}", assignments2.size(), assignments2.stream().map(Object::toString).collect(Collectors.joining("\n")));

        log.info("Building the FedQPL query…");
        Op asFedQPL = SA2FedQPL.build(queryAsOp, assignments2, tsst.tqt);

        log.info("Optimizing the resulting FedQPL plan…");
        // TODO more optimizations and simplifications, if need be
        Op before = null;
        while (Objects.isNull(before) || !before.equalTo(asFedQPL, new NodeIsomorphismMap())) { // should converge
            before = asFedQPL;
            asFedQPL = ReturningOpVisitorRouter.visit(new FedQPLSimplifyVisitor(), asFedQPL);
            asFedQPL = ReturningOpVisitorRouter.visit(new FedQPLWithExclusiveGroupsVisitor(), asFedQPL);
        }
        // log.debug("FedUP plan:\n{}", asFedQPL.toString());
        return asFedQPL;
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

    static List<Map<Var, String>> removeInclusions(List<Map<Var, String>> sourceSelection) {
        List<Map<Var, String>> withoutDuplicates = new ArrayList<>();
        for (Map<Var, String> e1 : sourceSelection) {
            if (!(withoutDuplicates.contains(e1))) {
                withoutDuplicates.add(e1);
            }
        }

        List<Map<Var, String>> newSourceSelection = new ArrayList<>();
        for (int i = 0; i < withoutDuplicates.size(); i++) {
            boolean keep = true;
            for (int j = 0; j < withoutDuplicates.size(); j++) {
                if (i != j && withoutDuplicates.get(j).entrySet().containsAll(withoutDuplicates.get(i).entrySet())) {
                    keep = false;
                    break;
                }
            }
            if (keep) {
                newSourceSelection.add(withoutDuplicates.get(i));
            }
        }
        return newSourceSelection;
    }

}
