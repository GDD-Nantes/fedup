package fr.gdd.fedqpl;

import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.QueryEngineTDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * We store the source assignments in a dataset to be queried.
 */
public class SAAsKG {

    final Dataset dataset;
    final ToQuadsTransform tqt;
    final HashMap<Op, MultiSet<Binding>> cacheOp2Sols = new HashMap<>();

    public SAAsKG(ToQuadsTransform tqt, List<Map<Var, String>> assignments) {
        this.tqt = tqt;

        this.dataset = TDB2Factory.createDataset();  // TODO built using a CONSTRUCT query
        dataset.begin(ReadWrite.WRITE);
        int rowNb = 0;
        // List<Statement> statements = new ArrayList<>();
        Map<String, List<Statement>> model2statements = new HashMap<>();
        for (Map<Var, String> assignment : assignments) {
            rowNb += 1;

            for (Map.Entry<Var, String> var2source : assignment.entrySet()) {
                var stmts = model2statements.computeIfAbsent(var2source.getValue(), k -> new ArrayList<>());

                stmts.add(new StatementImpl(
                        ResourceFactory.createResource(var2source.getKey().getVarName()),
                        ResourceFactory.createProperty("row"),
                        ResourceFactory.createResource(String.valueOf(rowNb))
                ));
            }
        }
        model2statements.forEach((k, v) -> {dataset.getNamedModel(k).add(v);});
        dataset.commit();
        dataset.end();
    }

    /**
     * @param op The operator to retrieve the matching solution from.
     * @return The solution mapping containing the graph for source assignement based on the
     *         operator in argument.
     */
    public MultiSet<Binding> sols (Op op) {
        if (cacheOp2Sols.containsKey(op)) {
            return cacheOp2Sols.get(op);
        }

        MultiSet<Binding> bindings = new HashMultiSet<>();
        boolean inTxn = dataset.isInTransaction();
        if (!inTxn) dataset.begin(ReadWrite.READ);

        Op checking = ReturningOpVisitorRouter.visit(new Op2SAChecker(tqt), op);

        // previously was using QueryEngineMain but was way slower than QueryEngineTDB
        Plan plan = QueryEngineTDB.getFactory().create(checking,
                dataset.asDatasetGraph(),
                BindingRoot.create(),
                dataset.getContext().copy());

        QueryIterator iterator = plan.iterator();

        while (iterator.hasNext()) {
            bindings.add(iterator.next());
        }

        if (!inTxn) {
            dataset.commit();
            dataset.end();
        }

        cacheOp2Sols.put(op, bindings);
        return bindings;
    }

    /**
     * Checks if the Op seems to have at least one solution, based on the source assignment.
     */
    public boolean ask(Op op) {
        boolean inTxn = dataset.isInTransaction();
        if (!inTxn) dataset.begin(ReadWrite.READ);

        Op checking = ReturningOpVisitorRouter.visit(new Op2SAChecker(tqt), op);

        Plan plan = QueryEngineTDB.getFactory().create(checking,
                dataset.asDatasetGraph(),
                BindingRoot.create(),
                dataset.getContext().copy());

        QueryIterator iterator = plan.iterator();

        boolean result = iterator.hasNext();

        if (!inTxn) {
            dataset.commit();
            dataset.end();
        }

        return result;
    }
}
