package fr.gdd.fedup.transforms;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.*;

import java.util.List;

/**
 * Throws at runtime when not implemented, so it does not go
 * in silent unknown behavior.
 */
public class TransformUnimplemented implements Transform {

    @Override
    public Op transform(OpTable opTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpBGP opBGP) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpTriple opTriple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpQuad opQuad) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpPath opPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpDatasetNames opDatasetNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpQuadPattern opQuadPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpQuadBlock opQuadBlock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpNull opNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpFilter opFilter, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpGraph opGraph, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpService opService, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpProcedure opProcedure, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpPropFunc opPropFunc, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpLabel opLabel, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpAssign opAssign, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpExtend opExtend, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpJoin opJoin, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpDiff opDiff, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpMinus opMinus, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpUnion opUnion, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpLateral opLateral, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpConditional opConditional, Op op, Op op1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpSequence opSequence, List<Op> list) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpDisjunction opDisjunction, List<Op> list) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpExt opExt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpList opList, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpOrder opOrder, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpTopN opTopN, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpProject opProject, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpDistinct opDistinct, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpReduced opReduced, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpSlice opSlice, Op op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Op transform(OpGroup opGroup, Op op) {
        throw new UnsupportedOperationException();
    }
}
