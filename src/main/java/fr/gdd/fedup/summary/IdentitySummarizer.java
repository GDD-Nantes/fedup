package fr.gdd.fedup.summary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Quad;

public class IdentitySummarizer extends Summarizer {

    public IdentitySummarizer(Integer arg) {
        super(arg);
    }

    @Override
    public Node summarize(Node node) {
        return node;    
    }

    @Override
    public Triple summarize(Triple triple) {
        return triple;
    }

    @Override
    public Quad summarize(Quad quad) {
        return quad;
    }

    @Override
    public Query summarize(Query query) {
        return query;
    }
    
}
