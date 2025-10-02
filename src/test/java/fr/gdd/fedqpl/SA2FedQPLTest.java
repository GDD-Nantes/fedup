package fr.gdd.fedqpl;

import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.IM4LabelSummaryFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class SA2FedQPLTest {

    private static final Logger log = LoggerFactory.getLogger(SA2FedQPLTest.class);

    @Test
    public void test_join_match_on_all_graphs_do_not_produce_cartesian_product(){
        IM4LabelSummaryFactory factory = new IM4LabelSummaryFactory();
        FedUP fedup = new FedUP(factory.getSimplePeopleSummary(), factory.getPeopleDataset());

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
        Assertions.assertEquals(result, resultForceJoin);

        factory.close();
    }

    @Test
    public void test_left_join_match_on_all_graphs_do_not_produce_cartesian_product(){
        IM4LabelSummaryFactory factory = new IM4LabelSummaryFactory();
        FedUP fedup = new FedUP(factory.getSimplePeopleSummary(), factory.getPeopleDataset());

        String queryAsString = """
                SELECT * WHERE {
                    ?s <http://auth/type> <http://auth/person>.
                    OPTIONAL {?s <http://auth/label> ?label.}
                }
                """;

        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));

        Op result = fedup.queryToFedQPL(queryAsOp, factory.getSimplePeopleSummary().getGraphs());
        result = ReturningOpVisitorRouter.visit(new FedQPL2SPARQL(), result);

        Pattern pattern = Pattern.compile("service");
        Assertions.assertEquals(2, pattern.matcher(result.toString()).results().count());

        factory.close();
    }
}
