package fr.gdd.fedqpl.transformers;

import fr.gdd.fedqpl.operators.Mu;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.TransformCopy;

public class TransformFedQPLJena extends TransformCopy {
    Query query;

    public TransformFedQPLJena() {
        query = QueryFactory.create();
    }

    public Op transform(Mu opMu) {
        
    }

}
