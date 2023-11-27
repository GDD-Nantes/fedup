package fr.gdd.fedup;

import fr.gdd.fedqpl.FedQPL2SPARQL;
import fr.gdd.fedqpl.SA2FedQPL;
import fr.gdd.fedqpl.groups.FedQPLWithExclusiveGroupsVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.ToSourceSelectionTransforms;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.reasoner.rulesys.builtins.Sum;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQueryMore;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The main class of FedUP that build logical query plans for queries over
 * federations of SPARQL endpoints. Therefore, it only takes care of source selection,
 * not federated execution.
 */
public class FedUP {

    Logger log = LoggerFactory.getLogger(FedUP.class);

    // The quotient summary to retrieve possibly relevant sources.
    final Summary summary;
    Dataset ds4Asks = null; // mostly for testing purposes

    public FedUP (Summary summary) {
        this.summary = summary;
    }

    /**
     * Mainly for testing purposes.
     * @param summary The summary of the federation graphs.
     * @param ds4Asks The full dataset of the federation graphs (to perform ASKS on it).
     */
    public FedUP (Summary summary, Dataset ds4Asks) {
        this.summary = summary;
        this.ds4Asks = ds4Asks;
    }

    /* ************************************************************** */

    /**
     * @param queryAsString The federated query to execute.
     * @param endpoints The set of SPARQL endpoints.
     * @return A SPARQL 1.1 query with SERVICE clauses to query remote endpoints.
     */
    public String query(String queryAsString, Set<String> endpoints) {
        log.debug("Parsing the query {}", queryAsString);
        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));


        log.info("Start making ASK queries…");
        // TODO use summary as first filter for ASKS
        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(summary.getStrategy(), true, endpoints, ds4Asks);
        Op ssQueryAsOp = tsst.transform(queryAsOp);

        log.info("Start executing the source selection query…");
        summary.getSummary().begin(ReadWrite.READ);
        QueryIterator iterator = Algebra.exec(ssQueryAsOp, summary.getSummary());

        // TODO could be processed using a provenance query
        List<Map<Var, String>> assignments = new ArrayList<>();
        Set<Integer> seen = new TreeSet<>();
        while (iterator.hasNext()) {
            // TODO create FedQPL here
            // TODO but it's much more difficult in presence of OPTIONAL
            // TODO but could get faster time for first result when things are sure
            Binding binding = iterator.next();
            int hashcode = binding.toString().hashCode();
            if (!seen.contains(hashcode)) {
                seen.add(hashcode);
                assignments.add(bindingToMap(binding));
            }
        }
        summary.getSummary().commit();
        summary.getSummary().end();

        log.info("Removing duplicates and inclusions in logical plan…");
        assignments = removeInclusions(assignments); // TODO double check if it can be improved
        log.debug("Assignments:\n{}", assignments.stream().map(Object::toString).collect(Collectors.joining("\n")));

        log.info("Building the FedQPL query…");
        Op asFedQPL = SA2FedQPL.build(queryAsOp, assignments, tsst.tqt);

        log.info("Optimizing the resulting FedQPL plan…");
        // TODO more optimizations and simplifications, if need be
        asFedQPL = ReturningOpVisitorRouter.visit(new FedQPLWithExclusiveGroupsVisitor(), asFedQPL);

        log.info("Building the SPARQL SERVICE query…");
        Op asSPARQL = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), asFedQPL);
        String asSERVICE = OpAsQueryMore.asQuery(asSPARQL).toString();

        log.info("Built the following query:\n{}", asSERVICE);
        return asSERVICE;
    }

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
