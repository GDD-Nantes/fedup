package fr.gdd.fedqpl.visitors;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.op.*;

public class OpVisitorRouter {
    public static void visit(OpVisitor t, Op op) {

        if ( op instanceof OpService o ) { t.visit(o); } else
        if ( op instanceof OpTriple o ) { t.visit(o); } else
        if ( op instanceof OpQuad o ) { t.visit(o); } else
        if ( op instanceof OpBGP o ) { t.visit(o); } else
        if ( op instanceof OpSequence o ) { t.visit(o); } else
        if ( op instanceof OpTable o ) { t.visit(o); } else
        if ( op instanceof OpLeftJoin o ) { t.visit(o); } else
        if ( op instanceof OpConditional o ) { t.visit(o); } else
        if ( op instanceof OpFilter o ) { t.visit(o); } else
        if ( op instanceof OpDistinct o ) { t.visit(o); } else
        if ( op instanceof OpUnion o ) { t.visit(o); } else
        if ( op instanceof OpJoin o ) { t.visit(o); } else

        if ( op instanceof OpSlice o ) { t.visit(o); } else
        if ( op instanceof OpOrder o ) { t.visit(o); } else
        if ( op instanceof OpProject o ) { t.visit(o); } else
        if ( op instanceof OpGroup o ) { t.visit(o); } else
        
        throw new UnsupportedOperationException(op.toString());
    }
}