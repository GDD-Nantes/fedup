package fr.gdd.fedqpl.visitors;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Simple way to provide clones with new child(ren).
 */
public class OpCloningUtil {
    public static OpDistinct clone(OpDistinct distinct, Op subOp) {return new OpDistinct(subOp);}
    public static OpSlice clone(OpSlice slice, Op subOp) {return new OpSlice(subOp, slice.getStart(), slice.getLength());}
    public static OpOrder clone (OpOrder orderBy, Op subOp) {return new OpOrder(subOp, orderBy.getConditions());}
    public static OpProject clone (OpProject project, Op subOp) {return new OpProject(subOp, project.getVars());}
    public static OpFilter clone(OpFilter filter, Op subOp) {return OpFilter.filterDirect(filter.getExprs(), subOp);}
}
