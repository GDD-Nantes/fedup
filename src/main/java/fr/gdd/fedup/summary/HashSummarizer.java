package fr.gdd.fedup.summary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

import java.net.URI;
import java.net.URISyntaxException;

public class HashSummarizer {

    public static Node summarize(Node node, int modulo) {
        if (node.isURI()) {
            try {
                URI uri = new URI(node.getURI());
                int hashcode = Math.abs(uri.toString().hashCode());
                return NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost() + "/" + (hashcode % modulo));
            } catch (URISyntaxException e) {
                return NodeFactory.createURI("https://donotcare.com/whatever");
            }
        } else if (node.isLiteral()) {
            return NodeFactory.createLiteral("any");
        } else {
            return NodeFactory.createVariable(node.getName()); 
        }
    }

    public static Triple summarize(Triple triple, int modulo) {
        Node subject = summarize(triple.getSubject(), modulo);
        Node predicate = triple.getPredicate();
        Node object = summarize(triple.getObject(), modulo);
        return Triple.create(subject, predicate, object);
    }

    public static Quad summarize(Quad quad, int modulo) {
        Node graph = quad.getGraph();
        Node subject = summarize(quad.getSubject(), modulo);
        Node predicate = quad.getPredicate();
        Node object = summarize(quad.getObject(), modulo);
        return Quad.create(graph, subject, predicate, object);
    }

    /*
    public Query summarize(Query query) {
        Op op = Algebra.compile(query);
        OpWalker.walk(op, new OpVisitorBase() {
            @Override
            public void visit(final OpBGP opBGP) {
                List<Triple> triples = opBGP.getPattern().getList().stream().map(triple -> {
                    return summarize(triple);
                }).collect(Collectors.toList());
                opBGP.getPattern().getList().clear();
                opBGP.getPattern().getList().addAll(triples);
            }
        });
        String queryString = OpAsQuery.asQuery(op).toString();
        queryString = queryString.replaceAll("(FILTER|filter).*", "");
        return QueryFactory.create(queryString);
        // return OpAsQuery.asQuery(op);
    }*/

}