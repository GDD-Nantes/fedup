package fr.gdd.fedup;

import fr.gdd.fedup.transforms.TransformUnimplemented;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.eclipse.rdf4j.query.algebra.Str;

import java.util.HashMap;
import java.util.List;

/**
 * Convert source assignments & logical plan into a service query.
 */
public class SA2ServiceQuery extends TransformUnimplemented {

    HashMap<Var, String> assignment;

    public void SA2ServiceQuery(HashMap<Var, String> assignment) {
        this.assignment = assignment;
    }

    @Override
    public Op transform(OpTriple opTriple) {
        return super.transform(opTriple);
    }
}
