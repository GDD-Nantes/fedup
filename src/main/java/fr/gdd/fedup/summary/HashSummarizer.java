package fr.gdd.fedup.summary;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.Quad;

public class HashSummarizer extends Summarizer {

    private int modulo;

    public HashSummarizer(Integer arg) {
        super(arg);
        this.modulo = arg;
    }
    
    @Override
    public Node summarize(Node node) {
        if (node.isURI()) {
            try {
                URI uri = new URI(node.getURI());
                if (this.modulo == 0) {
                    return NodeFactory.createURI("http://" + uri.getHost());
                } else {
                    int hashcode = Math.abs(uri.toString().hashCode());
                    return NodeFactory.createURI("http://" + uri.getHost() + "/" + (hashcode % this.modulo));
                }
            } catch (URISyntaxException e) {
                return NodeFactory.createURI("http://donotcare.com/whatever");
            }
        } else if (node.isLiteral()) {
            return NodeFactory.createLiteral("any");
        } else {
            return NodeFactory.createVariable(node.getName()); 
        }
    }

    @Override
    public Triple summarize(Triple triple) {
        Node subject = this.summarize(triple.getSubject());
        Node predicate = triple.getPredicate();
        Node object = this.summarize(triple.getObject());
        // Node object;
        // if (predicate.isURI() && predicate.getURI().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
        //     object = triple.getObject();
        // } else {
        //     object = this.summarize(triple.getObject());
        // }
        return Triple.create(subject, predicate, object);
    }

    @Override
    public Quad summarize(Quad quad) {
        Node graph = quad.getGraph();
        Node subject = this.summarize(quad.getSubject());
        Node predicate = quad.getPredicate();
        Node object = this.summarize(quad.getObject());
        // Node object;
        // if (predicate.isURI() && predicate.getURI().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
        //     object = quad.getObject();
        // } else {
        //     object = this.summarize(quad.getObject());
        // }
        return Quad.create(graph, subject, predicate, object);
    }

    @Override
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
    }

}