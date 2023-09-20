package fr.gdd.fedup.source.selection;

import fr.gdd.fedup.source.selection.transforms.ToQuadsTransform;
import fr.gdd.fedup.summary.InMemorySummaryFactory;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QuadBuilderFromBindingAndPlanTest {

    Logger log = LoggerFactory.getLogger(QuadBuilderFromBindingAndPlanTest.class);

    @Test
    public void simple_rebuilding_quads_from_query_and_results () {
        InMemorySummaryFactory.getSimplePetsSummary();
        Dataset dataset = InMemorySummaryFactory.getPetsDataset();

        String queryAsString = "SELECT * WHERE {<http://auth/person> ?p ?o}";
        Query query = QueryFactory.create(queryAsString);
        Op op = Algebra.compile(query);
        op = Transformer.transform(new ToQuadsTransform(), op);

        dataset.begin(TxnType.READ);
        ExecutionContext ec = new ExecutionContext(dataset.asDatasetGraph());
        OpExecutor executor = OpExecutorTDB2.stdFactory.create(ec);
        QueryIterator qi = executor.executeOp(op, OpExecutor.createRootQueryIterator(ec));

        List<Quad> quads = new ArrayList<>();
        while (qi.hasNext()) {
            Binding b = qi.next();
            QuadBuilderFromBindingAndPlan rebuild = new QuadBuilderFromBindingAndPlan(b);
            op.visit(rebuild);
            quads.addAll(rebuild.getQuads());
        }

        quads.forEach( e -> log.debug(e.toString()));
        assertEquals(4, quads.size());

        dataset.end();
    }

}