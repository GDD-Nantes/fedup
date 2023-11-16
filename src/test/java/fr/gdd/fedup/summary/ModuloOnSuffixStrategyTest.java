package fr.gdd.fedup.summary;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.junit.jupiter.api.Test;

class ModuloOnSuffixStrategyTest {

    @Test
    public void query_transformer_adds_graph_clauses_and_projects_all_variables_and_removes_limits() {
        String queryString = """
                SELECT ?predicate ?object WHERE {
                    {
                        <https://dbpedia.org/resource/Barack_Obama> ?predicate ?object .
                    } UNION {
                        ?subject <https://www.w3.org/2002/07/owl#sameAs> <https://dbpedia.org/resource/Barack_Obama> .
                        ?subject ?predicate ?object .
                    }
                } LIMIT 5""";
        Query query = QueryFactory.create(queryString);
        Op queryOp = Algebra.compile(query);

        ModuloOnSuffix queryTransformer = new ModuloOnSuffix(1);
        Op transformedQueryOp = Transformer.transform(queryTransformer, queryOp);

        System.out.println(transformedQueryOp.toString());
    }
}