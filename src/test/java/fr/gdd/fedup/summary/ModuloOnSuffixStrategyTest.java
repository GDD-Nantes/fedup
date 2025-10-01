package fr.gdd.fedup.summary;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ModuloOnSuffixStrategyTest {

    private static Logger log = LoggerFactory.getLogger(ModuloOnSuffixStrategyTest.class);

    @Test
    public void query_transformer_adds_graph_clauses_and_projects_all_variables_and_removes_limits() {
        final String queryString = """
                SELECT ?predicate ?object WHERE {
                    {
                        <https://dbpedia.org/resource/Barack_Obama> ?predicate ?object .
                    } UNION {
                        ?subject <https://www.w3.org/2002/07/owl#sameAs> <https://dbpedia.org/resource/Barack_Obama> .
                        ?subject ?predicate ?object .
                    }
                } LIMIT 5""";
        final Query query = QueryFactory.create(queryString);
        final Op queryOp = Algebra.compile(query);

        final ModuloOnSuffix queryTransformer = new ModuloOnSuffix(1);
        final Op transformedQueryOp = Transformer.transform(queryTransformer, queryOp);

        log.debug("Transformed: {}", transformedQueryOp.toString());
    }
}