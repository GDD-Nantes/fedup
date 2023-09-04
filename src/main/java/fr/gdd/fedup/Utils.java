package fr.gdd.fedup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.query.QueryFactory;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.structures.Endpoint;

public class Utils {

    public static Endpoint getEndpointByURL(List<Endpoint> endpoints, String url) {
        for (Endpoint endpoint: endpoints) {
            if (endpoint.getEndpoint().contains(url)) {
                return endpoint;
            }
        }
        return null;
    }

    public static <K, V> Map<K, Set<V>> convertListofMapToMapofList(List<Map<K, List<V>>> list) {
        Map<K, Set<V>> resultMap = new HashMap<>();
        list.forEach(map -> {
            map.forEach((key, value) -> {
                resultMap.computeIfAbsent(key, k -> new HashSet<>()).addAll(value);
            });
        });
        return resultMap;
    }

    public static List<List<StatementPattern>> getBasicGraphPatterns(ParsedQuery parsedQuery) throws Exception {
        try {
            List<List<StatementPattern>> bgps = new ArrayList<>();

            AbstractQueryModelVisitor<Exception> visitor = new AbstractQueryModelVisitor<Exception>() {
                
                private List<StatementPattern> currentBGP = new ArrayList<>();
                
                private void flush() {
                    if (this.currentBGP.size() > 0) {
                        bgps.add(this.currentBGP);
                        this.currentBGP = new ArrayList<>();
                    }
                }

                @Override
                public void meetOther(QueryModelNode node) throws Exception {
                    super.meetOther(node);
                    this.flush();
                }
        
                @Override
                public void meet(Union node) throws Exception {
                    this.flush();
                    node.getLeftArg().visit(this);
                    this.flush();
                    node.getRightArg().visit(this);
                    this.flush();
                }
        
                @Override
                public void meet(LeftJoin node) throws Exception {
                    this.flush();
                    node.getLeftArg().visit(this);
                    this.flush();
                    node.getRightArg().visit(this);
                    this.flush();
                }
        
                @Override
                public void meet(StatementPattern node) throws Exception {
                    this.currentBGP.add(node);
                }
            };

            visitor.meetOther(parsedQuery.getTupleExpr());

            return bgps;
        } catch (Exception e) {
            throw new Exception("Error when extracting basic graph patterns", e.getCause());
        }
    }

    public static List<List<StatementPattern>> getBasicGraphPatterns(String queryString) throws Exception {
        ParsedQuery parseQuery = new SPARQLParser().parseQuery(queryString, "http://donotcare.com/wathever");
        return getBasicGraphPatterns(parseQuery);
    }

    public static List<StatementPattern> getTriplePatterns(String queryString) throws Exception {
        return getBasicGraphPatterns(queryString)
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    public static long computeTPWSS(List<Map<StatementPattern, List<StatementSource>>> assignments) {
        return convertListofMapToMapofList(assignments).values().stream()
            .mapToLong(column -> column.size())
            .sum();
    }

    public static long getLimit(String queryString) {
        long limit = QueryFactory.create(queryString).getLimit();
        if (limit <= 0) {
            return Long.MAX_VALUE;
        } else {
            return limit;
        }
    }

}
