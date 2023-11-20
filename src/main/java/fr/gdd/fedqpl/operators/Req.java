package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.List;
import java.util.Set;

/**
 * Request.
 */
public class Req implements FedQPLOperator {

    // TODO Not only List<Triple> but actually List<Op>
    List<Triple> triples;
    Node source;

    public Req(Triple triple, Node source) {
        this.triples = List.of(triple);
        this.source = source;
    }

    public Req(List<Triple> triples, Node source) {
        this.triples = triples;
        this.source = source;
    }

    public List<Triple> getTriples() {
        return triples;
    }

    public Node getSource() {
        return source;
    }

    @Override
    public <T> T visit(FedQPLVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
