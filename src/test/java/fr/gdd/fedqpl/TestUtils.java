package fr.gdd.fedqpl;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {
    public static class TestEqualOp extends ReturningArgsOpVisitor<Boolean, Op> {

        Map<String, String> correspondance = new HashMap<>();

        public Boolean visit(OpProject op, Op other) {
            if( ! (other instanceof OpProject) ) {
                return false;
            }

            if( ! ( op.getVars().equals(((OpProject)other).getVars()) ) ) return false;

            return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((OpProject) other).getSubOp());
        }

        public Boolean visit(OpJoin op, Op other) {
            if( ! (other instanceof OpJoin) ) {
                return false;
            }

            return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpJoin) other).getLeft())
                    && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpJoin) other).getRight())
                    ||
                    ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpJoin) other).getLeft())
                            && ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpJoin) other).getRight()) ;
        }

        public Boolean visit(OpLeftJoin op, Op other) {
            if( ! (other instanceof OpLeftJoin) ) {
                return false;
            }

            return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpLeftJoin) other).getLeft())
                    && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpLeftJoin) other).getRight());
        }

        public Boolean visit(OpUnion op, Op other) {
            if( ! (other instanceof OpUnion) ) {
                return false;
            }

            return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpUnion) other).getLeft())
                    && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpUnion) other).getRight())
                    ||
                    ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpUnion) other).getLeft())
                            && ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpUnion) other).getRight()) ;
        }

        public Boolean visit(OpBGP op, Op other) {
            if( other instanceof OpTriple) return ((OpTriple) other).equivalent(op);

            if( ! (other instanceof OpBGP) ) {
                return false;
            }

            return op.equalTo(other, null);
        }

        public Boolean visit(OpTriple op, Op other) {
            if( other instanceof OpBGP ) return op.equivalent((OpBGP) other);

            return op.equalTo(other, null);
        }

        public Boolean visit(OpService op, Op other) {
            if( ! (other instanceof OpService) ) {
                return false;
            }

            if(correspondance.containsKey(op.getService().toString())) {
                if(correspondance.get(op.getService().toString()) != ((OpService) other).getService().toString()) {
                    return false;
                }
            } else {
                correspondance.put(op.getService().toString(), ((OpService) other).getService().toString());
            }

            return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((OpService) other).getSubOp());
        }

        public Boolean visit(OpFilter op, Op other) {
            if( ! (other instanceof OpFilter otherFilter) ) {
                return false;
            }

            if ( ! op.getExprs().equals( otherFilter.getExprs() ) ) return false;

            return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((Op1) other).getSubOp());
        }

        public Boolean visit(OpSequence op, Op other) {
            if( ! (other instanceof OpSequence otherSequence) ) {
                return false;
            }

            return visitN(op.getElements(), otherSequence.getElements());
        }

        public Boolean visit(OpTable op, Op other) {
            if( ! (other instanceof OpTable otherTable) ) {
                return false;
            }

            return op.equalTo(otherTable, null);
        }

        public Boolean visitN(List<Op> ops1, List<Op> ops2) {
            if( ops1.size() != ops2.size() ) {
                return false;
            }

            for( int i = 0; i < ops1.size(); i++ ) {
                Op op1 = ops1.get(i);
                Op op2 = ops2.get(i);

                if(ReturningArgsOpVisitorRouter.visit(this, op1, op2)) return false;
            }
            return true;
        }
    }
}
