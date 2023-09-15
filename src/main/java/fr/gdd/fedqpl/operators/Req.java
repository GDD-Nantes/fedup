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

/**
 * Request.
 */
public class Req extends FedQPLOperator {

    Triple triple;
    Node source;

    public Req(Triple triple, Node source) {
        this.triple = triple;
        this.source = source;
    }

    public Triple getTriple() {
        return triple;
    }

    public Node getSource() {
        return source;
    }

    public Query toSPARQL(Query query){
        ElementTriplesBlock elmt_bgp = new ElementTriplesBlock();
        elmt_bgp.addTriple(triple);
        ElementNamedGraph elmt_graph = new ElementNamedGraph(source, elmt_bgp);

        if (query.getQueryPattern() == null) {
            query.setQueryPattern(elmt_graph);
        } else {
            ElementGroup elmt = (ElementGroup) query.getQueryPattern();
            elmt.addElement(elmt_graph);
            query.setQueryPattern(elmt);
        }

        return query;
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        // TODO Auto-generated method stub
        if (!(opVisitor instanceof FedQPLVisitor)) {
            throw new IllegalArgumentException("The visitor should be an instance of FedQPLVisitor");
        }
        FedQPLVisitor visitor = (FedQPLVisitor) opVisitor;
        visitor.visit(this);
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return "Req(" + triple.toString() + ", " + source.getURI() + ")";
    }

    @Override
    public int hashCode() {
        return this.triple.hashCode() << 1 ^ this.source.hashCode() ^ getName().hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        // TODO Auto-generated method stub
        if (!(other instanceof Req))
            return false;
        Req opJoin = (Req) other;
        return (opJoin.getTriple().equals(this.triple) && opJoin.getSource().equals(this.source) );
    }
}
