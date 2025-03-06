package fr.gdd.fedup.adapters;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.eclipse.rdf4j.federated.repository.FedXRepositoryConnection;
import org.eclipse.rdf4j.federated.structures.FedXTupleQuery;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;

import java.util.Objects;

/**
 * Adapter between FedX query results and Apache Jena bindings as iterator.
 * Of course, this is not very efficient and should be avoided when there are a lot
 * of results. In particular, the static function called `createBinding` parses the results,
 * therefore being slow.
 */
public class TupleQueryResult2QueryIterator implements QueryIterator {

    FedXRepositoryConnection conn;
    TupleQueryResult tqRes = null;

    public TupleQueryResult2QueryIterator(FedXRepositoryConnection conn, TupleExpr queryAsFedX) {
        this.conn = conn;
        TupleQuery tq = new FedXTupleQuery(new SailTupleQuery(new ParsedTupleQuery(queryAsFedX), this.conn));
        try {
            tqRes = tq.evaluate();
        } catch (Exception e) {
            this.close();
        }
    }

    @Override
    public boolean hasNext() {
        if (Objects.isNull(tqRes) || !conn.isOpen()) { return false; }

        try {
            return tqRes.hasNext();
        } catch (Exception e) {
            close();
            return false;
        }
    }

    @Override
    public Binding nextBinding() {
        if (Objects.isNull(tqRes) || !conn.isOpen()) {
            throw new RuntimeException("Connection closed while attempting to get results.");
        }

        try {
            Binding binding = createBinding(tqRes.next());
            return binding;
        } catch (Exception e) {
            close();
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Binding next() {
        return this.nextBinding();
    }

    @Override
    public void cancel() {conn.close();}

    @Override
    public void close() {this.conn.close();}

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {}

    @Override
    public String toString(PrefixMapping pmap) {
        return null;
    }

    @Override
    public void output(IndentedWriter out) {}


    /**
     * @param origin The RDF4J binding to convert.
     * @return An Apache Jena `Binding` that comes from an RDF4J binding.
     */
    public static org.apache.jena.sparql.engine.binding.Binding createBinding(BindingSet origin) {
        BindingBuilder builder = BindingFactory.builder();
        for (String name : origin.getBindingNames()) {
            org.eclipse.rdf4j.query.Binding value = origin.getBinding(name);
            Node valueAsNode = null;
            if (value.getValue().isBNode()) {
                valueAsNode = NodeFactory.createBlankNode(value.getValue().stringValue());
            } else if (value.getValue().isIRI()) {
                valueAsNode = NodeFactory.createURI(value.getValue().stringValue());
            } else if (value.getValue().isLiteral()) {
                if (value.getValue().toString().contains(XSDDatatype.XSDinteger.getURI())) {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue(), XSDDatatype.XSDinteger);
                } else if (value.getValue().toString().contains(XSDDatatype.XSDdouble.getURI())) {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue(), XSDDatatype.XSDdouble);
                } else if (value.getValue().toString().contains(XSDDatatype.XSDdateTime.getURI())) {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue(), XSDDatatype.XSDdateTime);
                } else {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue());
                }
            } else if (value.getValue().isResource() || value.getValue().isTriple()) {
                throw new UnsupportedOperationException("RDF4J to Jena Bindings with a resource or a triple.");
            }
            builder.add(Var.alloc(name), valueAsNode);
        }
        return builder.build();
    }
}
