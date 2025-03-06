package fr.gdd.fedqpl.visitors;

import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.op.*;

public class OpVisitorUnimplemented implements OpVisitor {

    @Override
    public void visit(OpBGP opBGP) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpQuadBlock quadBlock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpTriple opTriple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpQuad opQuad) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpPath opPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpTable opTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpNull opNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpProcedure opProc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpPropFunc opPropFunc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpFilter opFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpGraph opGraph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpService opService) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpDatasetNames dsNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpLabel opLabel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpAssign opAssign) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpExtend opExtend) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpUnfold opUnfold) { throw new UnsupportedOperationException(); }

    @Override
    public void visit(OpJoin opJoin) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpUnion opUnion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpDiff opDiff) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpMinus opMinus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpLateral opLateral) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpConditional opCondition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpSequence opSequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpDisjunction opDisjunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpList opList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpOrder opOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpProject opProject) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpReduced opReduced) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpSlice opSlice) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpGroup opGroup) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpTopN opTop) {
        throw new UnsupportedOperationException();
    }
}
