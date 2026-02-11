package fr.gdd.fedqpl;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.IM4LabelSummaryFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

public class SA2FedQPLWithValuesTest {

    private static final Logger log = LoggerFactory.getLogger(SA2FedQPLWithValuesTest.class);

    @Test
    public void test_nominal(){
        IM4LabelSummaryFactory factory = new IM4LabelSummaryFactory();
        FedUP fedup = new FedUP(factory.getSimplePeopleSummary(), factory.getPeopleDataset());
        fedup.shouldFactorize();
        fedup.shouldTryWithValuesFirst();

        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/type> <http://auth/person>.
                    ?s <http://auth/label> ?label.
                }
                """;

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Op assigned = fedup.queryJenaToJena(query);


        // SELECT ?s ?label WHERE {
        //     [VALUES]
        //     [SERVICES]
        // }
        AtomicReference<OpSequence> sequence = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> sequence.set(((OpSequence)((OpProject) assigned).getSubOp())));
        Assertions.assertInstanceOf(OpTable.class, sequence.get().get(0));
        Assertions.assertInstanceOf(OpService.class, sequence.get().get(1));

        // There are two (or more !) source combinations
        Assertions.assertTrue(((OpTable)((OpSequence)((OpProject) assigned).getSubOp()).get(0)).getTable().size() >= 2);
    }

    @Test
    public void test_join_match_on_all_graphs_do_not_produce_cartesian_product(){
        IM4LabelSummaryFactory factory = new IM4LabelSummaryFactory();
        FedUP fedup = new FedUP(factory.getSimplePeopleSummary(), factory.getPeopleDataset());
        fedup.shouldFactorize();
        fedup.shouldTryWithValuesFirst();

        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/type> <http://auth/person>.
                    ?s <http://auth/label> ?label.
                }
                """;

        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));

        Op result = fedup.queryToFedQPL(queryAsOp, factory.getSimplePeopleSummary().getGraphs());
        result = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), result);

        log.debug("result: {}", result);

        String queryForceJoinAsString = """
                SELECT * WHERE {
                    ?s <http://auth/type> <http://auth/person>.
                    {}
                    ?s <http://auth/label> ?label.
                }
                """;

        Op queryForceJoinAsOp = Algebra.compile(QueryFactory.create(queryForceJoinAsString));
        Op resultForceJoin = fedup.queryToFedQPL(queryForceJoinAsOp, factory.getSimplePeopleSummary().getGraphs());
        resultForceJoin = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), resultForceJoin);

        log.debug("result: {}", resultForceJoin);

        TestUtils.TestEqualOp equalityVisitor = new TestUtils.TestEqualOp();
        Assertions.assertTrue(ReturningArgsOpVisitorRouter.visit(equalityVisitor, result, resultForceJoin));
//        Assertions.assertEquals(result, resultForceJoin);

        factory.close();
    }

    @Test
    public void test_left_join_match_on_all_graphs_do_not_produce_cartesian_product(){
        IM4LabelSummaryFactory factory = new IM4LabelSummaryFactory();
        FedUP fedup = new FedUP(factory.getSimplePeopleSummary(), factory.getPeopleDataset());
        fedup.shouldFactorize();
        fedup.shouldTryWithValuesFirst();

        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/type> <http://auth/person>.
                    OPTIONAL {?s <http://auth/label> ?label.}
                }
                """;

        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));

        Op result = fedup.queryToFedQPL(queryAsOp, factory.getSimplePeopleSummary().getGraphs());
        result = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), result);

        System.out.println(result);

        Pattern pattern = Pattern.compile("service");
        Assertions.assertEquals(2, pattern.matcher(result.toString()).results().count());


        factory.close();
    }
}
