package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;

/**
 * A visitor of FedQPL expressions with a return type that can
 * be parametrized if need be, in a functional way.
 * Note: Does not need corresponding Router since FedQPLOperator is ours
 * and implements visit.
 */
public interface FedQPLVisitor<T> {
    default T visit(Mu mu) {throw new UnsupportedOperationException("Mu");}
    default T visit(Mj mj) {throw new UnsupportedOperationException("Mj");}
    default T visit(Req req) {throw new UnsupportedOperationException("Req");}
    default T visit(LeftJoin lj) {throw new UnsupportedOperationException("LeftJoin");}
    default T visit(Filter filter) {throw new UnsupportedOperationException("Filter");}
}

