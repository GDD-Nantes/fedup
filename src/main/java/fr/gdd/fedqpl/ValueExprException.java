package fr.gdd.fedqpl;

import org.eclipse.rdf4j.query.algebra.ValueExpr;

public class ValueExprException extends RuntimeException {

    ValueExpr expr;

    public ValueExprException(ValueExpr expr) {
        this.expr = expr;
    }

}
