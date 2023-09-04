package fr.gdd.fedup.sourceselection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.queryrender.sparql.SPARQLQueryRenderer;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.Util;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.structures.Endpoint;

import fr.gdd.sage.OpExecutorRandom;
import fr.gdd.sage.QueryEngineRandom;
import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;
import fr.univnantes.gdd.fedup.summary.Summarizer;

public class FedUPSourceSelectionPerformer extends SourceSelectionPerformer {

    private static Logger logger = LogManager.getLogger(FedUPSourceSelectionPerformer.class);

    public FedUPSourceSelectionPerformer(SailRepositoryConnection connection) throws Exception {
        super(connection);
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(
        String queryString, List<Map<String, String>> optimalAssignments, Spy spy
    ) throws Exception {
        Config config = this.connection.getFederation().getConfig();

        long timeout = Long.parseLong(config.getProperty("fedup.budget", "0"));

        Dataset dataset = TDB2Factory.connectDataset(config.getProperty("fedup.summary"));
        dataset.begin(TxnType.READ);

        QueryEngineFactory factory;
        if (Boolean.parseBoolean(config.getProperty("fedup.random", "false"))) {
            QC.setFactory(dataset.getContext(), new OpExecutorRandom.OpExecutorRandomFactory(ARQ.getContext()));
            factory = QueryEngineRandom.factory;
        } else {
            QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            factory = QueryEngineSage.factory;
        }
        QueryEngineRegistry.addFactory(factory);

        ImmutablePair<String, List<StatementPattern>> sourceSelectionQuery = this.createSourceSelectionQuery(queryString);

        logger.debug("Executing query...");
        long startTime = System.currentTimeMillis();

        Context context = dataset.getContext().copy();
        context.set(SageConstants.timeout, timeout == 0 ? Long.MAX_VALUE : timeout);

        Query query = QueryFactory.create(sourceSelectionQuery.getLeft());
        Plan plan = factory.create(query, dataset.asDatasetGraph(), BindingRoot.create(), context);
        QueryIterator iterator = plan.iterator();

        List<Map<String, String>> assignments = new ArrayList<>();
        Set<Integer> seen = new TreeSet<Integer>();

        logger.debug("Getting results...");
        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            int hashcode = binding.toString().hashCode();
            logger.debug("Binding #" +  Integer.toString(hashcode));
            if (!seen.contains(hashcode)) {
                seen.add(hashcode);
                assignments.add(this.bindingToMap(binding));
                if (optimalAssignments.size() > 0 && this.countMissingAssignments(assignments, optimalAssignments) == 0) {
                    break;
                }
            }
        }
        long endTime = System.currentTimeMillis();
        logger.debug("Query execution terminated...");

        spy.sourceSelectionTime = (endTime - startTime);

        assignments = this.removeInclusions(assignments);

        spy.assignments = assignments;
        spy.numAssignments = assignments.size();
        spy.numValidAssignments = optimalAssignments.size();
        spy.numFoundAssignments = optimalAssignments.size() - this.countMissingAssignments(assignments, optimalAssignments);

        List<Map<StatementPattern, List<StatementSource>>> fedXAssignments = new ArrayList<>();
        
        for (Map<String, String> assignment: assignments) {
            Map<StatementPattern, List<StatementSource>> fedXAssignment = new HashMap<>();
            for (int i = 1; i <= sourceSelectionQuery.getRight().size(); i++) {
                String alias = "g"+i;
                if (assignment.containsKey(alias)) {
                    Endpoint endpoint = Utils.getEndpointByURL(this.connection.getEndpoints(), assignment.get("g" + i));
                    StatementSource source = new StatementSource(endpoint.getId(), StatementSourceType.REMOTE);
                    StatementPattern pattern = sourceSelectionQuery.getRight().get(i - 1);
                    fedXAssignment.put(pattern, List.of(source));
                    spy.tpAliases.put(alias, pattern.toString());
                }
            }
            fedXAssignments.add(fedXAssignment);
        }

        dataset.end();
        dataset.close();

        return fedXAssignments;
    }

    private int countMissingAssignments(List<Map<String, String>> sourceSelection, List<Map<String, String>> optimalSourceSelection) {
        int missingAssignments = 0;
        for (Map<String, String> binding: optimalSourceSelection) {
            boolean found = false;
            for (Map<String, String> otherBinding: sourceSelection) {
                if (otherBinding.entrySet().containsAll(binding.entrySet())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missingAssignments += 1;
                // return false;
            }
        }
        // return true;
        return missingAssignments;
    }

    private Map<String, String> bindingToMap(Binding binding) {
        Map<String, String> bindingAsMap = new HashMap<>();
        Iterator<Var> vars = binding.vars();
        while (vars.hasNext()) {
            String varName = vars.next().getName();
            bindingAsMap.put(varName, binding.get(varName).getURI());
        }
        return bindingAsMap;
    }

    private List<Map<String, String>> removeInclusions(List<Map<String, String>> sourceSelection) {
        List<Map<String, String>> newSourceSelection = new ArrayList<>();
        for (int i = 0; i < sourceSelection.size(); i++) {
            boolean keep = true;
            for (int j = 0; j < sourceSelection.size(); j++) {
                if (i != j && sourceSelection.get(j).entrySet().containsAll(sourceSelection.get(i).entrySet())) {
                    keep = false;
                    break;
                }
            }
            if (keep) {
                newSourceSelection.add(sourceSelection.get(i));
            }
        }
        return newSourceSelection;
    }

    protected ImmutablePair<String, List<StatementPattern>> createSourceSelectionQuery(String queryString) throws Exception {
        try {
            queryString = new TriplePatternsReorderer().optimize(queryString);

            logger.debug("optimized query:\n" + queryString);

            List<StatementPattern> patterns = Utils.getTriplePatterns(queryString);

            ParsedQuery parseQuery = new SPARQLParser().parseQuery(queryString, "http://donotcare.com/wathever");

            List<ProjectionElem> projection = new ArrayList<>();

            AbstractQueryModelVisitor<Exception> visitor1 = new AbstractQueryModelVisitor<Exception>() {
                @Override
                public void meet(StatementPattern node) throws Exception {
                    int index = projection.size() + 1;
                    node.setContextVar(new org.eclipse.rdf4j.query.algebra.Var("g" + index));
                    projection.add(new ProjectionElem("g" + index));
                }
            };
            
            AbstractQueryModelVisitor<Exception> visitor2 = new AbstractQueryModelVisitor<Exception>() {
                @Override
                public void meet(ProjectionElemList node) throws Exception {
                    node.setElements(projection);
                }
            };

            visitor1.meetOther(parseQuery.getTupleExpr());
            visitor2.meetOther(parseQuery.getTupleExpr());
            
            queryString = new SPARQLQueryRenderer().render(parseQuery);
            queryString = queryString.replaceAll("(DISTINCT|distinct)", "");
            queryString = queryString.replaceAll("(ORDER BY|order by).*", "");
            queryString = queryString.replaceAll("(LIMIT|limit).*", "");
            // queryString = queryString.replaceAll("(FILTER|filter) \\(\\?date", "FILTER (<http://www.w3.org/2001/XMLSchema#dateTime>(CONCAT(STR(?date), \"\"\"T00:00:00\"\"\"))");
            // System.out.println(queryString);

            Config config = this.connection.getFederation().getConfig();
            
            Summarizer summarizer = (Summarizer) Util.instantiate(
                config.getProperty("fedup.summaryClass"),
            Integer.parseInt(config.getProperty("fedup.summaryArg", "0")));

            queryString = summarizer.summarize(QueryFactory.create(queryString)).toString();

            logger.debug("source selection query:\n" + queryString);

            return new ImmutablePair<>(queryString, patterns);
        } catch (Exception e) {
            throw e;
            // throw new Exception("Error when rewriting the query", e.getCause());
        }
    }

    private class TriplePatternsReorderer extends AbstractQueryModelVisitor<Exception> {

        private class StatementPatternWithScore {
            
            private StatementPattern pattern;

            public StatementPatternWithScore(StatementPattern pattern) {
                this.pattern = pattern;
            }

            public StatementPattern getPattern() {
                return this.pattern;
            }

            public int getScore() {
                int score = 0;
                if (this.pattern.getSubjectVar().isConstant()) {
                    score += 4;
                }
                if (this.pattern.getPredicateVar().isConstant() && !this.pattern.getPredicateVar().getValue().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                    score += 1;
                }
                if (this.pattern.getObjectVar().isConstant()) {
                    score += 2;
                }
                return score;
            }
        }

        private List<StatementPattern> currentBGP = new ArrayList<>();
        private List<String> currentVars = new ArrayList<>();

        private String optimize(String queryString) throws Exception {
            ParsedQuery parseQuery = new SPARQLParser().parseQuery(queryString, "http://donotcare.com/wathever");
            this.meetNode(parseQuery.getTupleExpr());
            this.reorderBGP();
            return new SPARQLQueryRenderer().render(parseQuery);
        }

        private boolean isConnected(StatementPattern pattern) {
            return pattern.getVarList().stream().anyMatch(var -> {
                return this.currentVars.contains(var.getName());
            });
        }

        private void updateVars(StatementPattern pattern) {
            List<String> vars = pattern.getVarList().stream().filter(var -> {
                return !var.isConstant();
            }).map(var -> {
                return var.getName();
            }).collect(Collectors.toList());
            this.currentVars.addAll(vars);
        }
                
        private void reorderBGP() throws Exception {
            List<StatementPatternWithScore> scoredTriples = this.currentBGP.stream().map(triple -> {
                return new StatementPatternWithScore(triple);
            }).collect(Collectors.toList());
            
            scoredTriples = scoredTriples.stream().sorted((a, b) -> {
                return b.getScore() - a.getScore();
            }).collect(Collectors.toList());
                        
            List<StatementPattern> orderedTriples = new ArrayList<>();
            while (scoredTriples.size() > 0) {
                boolean cartesianProduct = true;
                for (int i = 0; i < scoredTriples.size(); i++) {
                    if (this.isConnected(scoredTriples.get(i).getPattern())) {
                        this.updateVars(scoredTriples.get(i).getPattern());
                        orderedTriples.add(scoredTriples.remove(i).getPattern());
                        cartesianProduct = false;
                        break;
                    }
                }
                if (cartesianProduct) {
                    this.updateVars(scoredTriples.get(0).getPattern());
                    orderedTriples.add(scoredTriples.remove(0).getPattern());
                }
            }

            for (int i = 0; i < this.currentBGP.size(); i++) {
                this.currentBGP.get(i).replaceWith(orderedTriples.get(i).clone());
            }

            this.currentBGP = new ArrayList<>();
        }

        @Override
        public void meet(Union node) throws Exception {
            node.getLeftArg().visit(this);
            this.reorderBGP();
            this.currentVars = new ArrayList<>();
            node.getRightArg().visit(this);
            this.reorderBGP();
        }

        @Override
        public void meet(LeftJoin node) throws Exception {
            node.getLeftArg().visit(this);
            this.reorderBGP();
            node.getRightArg().visit(this);
            this.reorderBGP();
        }

        @Override
        public void meet(StatementPattern node) throws Exception {
            this.currentBGP.add(node);
        }
    }
}
