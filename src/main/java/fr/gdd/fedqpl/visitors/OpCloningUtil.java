package fr.gdd.fedqpl.visitors;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.List;

/**
 * Simple way to provide clones with new child(ren). All in one place.
 */
public class OpCloningUtil {
    public static OpService clone(OpService service, Op subOp) {return new OpService(service.getService(), subOp, service.getSilent());}
    public static OpDistinct clone(OpDistinct distinct, Op subOp) {return new OpDistinct(subOp);}
    public static OpSlice clone(OpSlice slice, Op subOp) {return new OpSlice(subOp, slice.getStart(), slice.getLength());}
    public static OpOrder clone (OpOrder orderBy, Op subOp) {return new OpOrder(subOp, orderBy.getConditions());}
    public static OpProject clone (OpProject project, Op subOp) {return new OpProject(subOp, project.getVars());}
    public static OpFilter clone(OpFilter filter, Op subOp) {return OpFilter.filterDirect(filter.getExprs(), subOp);}
    public static OpGroup clone(OpGroup group, Op subOp) {return new OpGroup(subOp, group.getGroupVars(), group.getAggregators());}
    public static OpUnion clone(OpUnion union, Op left, Op right) {return new OpUnion(left, right);}

    public static OpSequence clone(OpSequence sequence, List<Op> subops) {
        return (OpSequence) OpSequence.create().copy(subops);
    }

    public static OpLeftJoin clone(OpLeftJoin lj, Op left, Op right) {
        return OpLeftJoin.createLeftJoin(left, right, lj.getExprs());
    }

    public static OpConditional clone(OpConditional lj, Op left, Op right) {
        return new OpConditional(left, right);
    }
}
