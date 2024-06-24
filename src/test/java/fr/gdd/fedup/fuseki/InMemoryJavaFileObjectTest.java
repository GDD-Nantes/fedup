package fr.gdd.fedup.fuseki;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InMemoryJavaFileObjectTest {

    @Disabled
    @Test
    public void create_dynamically_a_lambda_doubling_a_number () {
        Function<Integer, Integer> doubleNumber = InMemoryLambdaJavaFileObject.getLambda("DoubleNumber", "x -> x * 2", "Integer");
        Integer twenty = doubleNumber.apply(10);  // Output should be 10 if the expression is "x -> x * 2"
        assertEquals(20, twenty);
    }

    @Disabled
    @Test
    public void call_virtuoso_removing_the_trailing_slash () {
        Function<String, String> virtuosoNoSlash = InMemoryLambdaJavaFileObject.getLambda(
                "VirtuosoNoSlash",
                "e -> \"http://localhost:5555/sparql?default-graph-uri=\" + (e.substring(0, e.length() - 1))",
                "String");
        String noSlash = virtuosoNoSlash.apply("http://www.ratingsite1.fr/");  // Output should be 10 if the expression is "x -> x * 2"
        assertEquals("http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite1.fr", noSlash);
    }

    @Disabled
    @Test
    public void call_virtuoso_as_is () {
        Function<String, String> virtuosoNoSlash = InMemoryLambdaJavaFileObject.getLambda(
                "VirtuosoNoSlash",
                "e -> \"http://localhost:5555/sparql?default-graph-uri=\" + e",
                "String");
        String noSlash = virtuosoNoSlash.apply("http://example.com/LinkedTCGA-A");  // Output should be 10 if the expression is "x -> x * 2"
        assertEquals("http://localhost:5555/sparql?default-graph-uri=http://example.com/LinkedTCGA-A", noSlash);
    }

}