package org.apache.jena.sparql.algebra;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.syntax.ElementQuadsBlock;

/**
 * Small extension of {@link OpAsQuery} to add missing behavior.
 */
public class OpAsQueryMore extends OpAsQuery {

    public static Query asQuery(Op op) {
        ConvertMore converter = new ConvertMore(op);
        return converter.convert();
    }

    public static class ConvertMore extends OpAsQuery.Converter {

        public ConvertMore(Op op) {
            super(op);
        }

        @Override
        public void visit(OpNull opNull) {
            // do nothing
        }

        @Override
        public void visit(OpQuad opQuad) {
            // ElementQuadsBlock block = new ElementQuadsBlock();
            // block.addQuad(opQuad.getQuad());
            // TODO currentgroup.add
            super.visit(opQuad); // throws
        }

        @Override
        public void visit(OpQuadPattern quadPattern) {
            // TODO this is important, but it is not implemented in Apache Jena yet
            super.visit(quadPattern); // throws
        }
    }

}
