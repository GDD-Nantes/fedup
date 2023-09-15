package fr.gdd.fedqpl.operators;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.op.OpBase;
import fr.gdd.fedqpl.visitors.FedQPLVisitor;

/**
 * Basic operator of FedQPL.
 */
public abstract class FedQPLOperator extends OpBase {
    
    public Object visit(FedQPLVisitor visitor) { throw new UnsupportedOperationException();}
    public Query toSPARQL(Query query) { throw new UnsupportedOperationException();}
}
