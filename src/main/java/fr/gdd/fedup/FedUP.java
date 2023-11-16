package fr.gdd.fedup;

import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.transforms.ToSourceSelectionTransforms;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The main class of FedUP that build logical query plans for queries over
 * federations of SPARQL endpoints. Therefore, it only takes care of source selection,
 * not federated execution.
 */
public class FedUP {

    Logger log = LoggerFactory.getLogger(FedUP.class);

    // The quotient summary to retrieve possibly relevant sources.
    final Dataset summary;
    final Dataset ds4Asks; // TODO

    public FedUP (String pathToSummary, String pathToId) {
        log.debug("Loading the summary file %s", pathToSummary);
        this.summary = TDB2Factory.connectDataset(pathToSummary);
        this.ds4Asks = TDB2Factory.connectDataset(pathToId);
    }

    public FedUP (Summary summary, Dataset ds4Asks) {
        this.summary = summary.getSummary();
        this.ds4Asks = ds4Asks;
    }

    /**
     * @param queryAsString The federated query to execute.
     * @param endpoints The set of SPARQL endpoints.
     * @return A SPARQL 1.1 query with SERVICE clauses to query remote endpoints.
     */
    public String query(String queryAsString, Set<String> endpoints) {
        log.debug("Parsing the query %s", queryAsString);
        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));


        log.info("Start making ASK queries…");
        ModuloOnSuffix hs = new ModuloOnSuffix(1); // TODO builder
        // TODO use summary as first filter for ASKS
        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(hs, true, endpoints, ds4Asks);
        Op ssQueryAsOp = tsst.transform(queryAsOp);

        log.info("Start executing the source selection query…");
        summary.begin(ReadWrite.READ);
        QueryIterator iterator = Algebra.exec(ssQueryAsOp, summary);

        List<Map<String, String>> assignments = new ArrayList<>();
        Set<Integer> seen = new TreeSet<>();
        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            int hashcode = binding.toString().hashCode();
            if (!seen.contains(hashcode)) {
                seen.add(hashcode);
                assignments.add(bindingToMap(binding));
            }
        }
        summary.commit();
        summary.end();

        log.info("Building the SERVICE query…");


        return null;
    }

    static Map<String, String> bindingToMap(Binding binding) {
        Map<String, String> bindingAsMap = new HashMap<>();
        Iterator<Var> vars = binding.vars();
        while (vars.hasNext()) {
            String varName = vars.next().getName();
            bindingAsMap.put(varName, binding.get(varName).toString());
        }
        return bindingAsMap;
    }

    static List<Map<String, String>> removeInclusions(List<Map<String, String>> sourceSelection) {
        List<Map<String, String>> withoutDuplicates = new ArrayList<>();
        for (Map<String, String> e1 : sourceSelection) {
            if (!(withoutDuplicates.contains(e1))) {
                withoutDuplicates.add(e1);
            }
        }

        List<Map<String, String>> newSourceSelection = new ArrayList<>();
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
