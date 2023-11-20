package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;

/**
 * A visitor of FedQPL expressions with a return type that can
 * be parametrized if need be, in a functional way.
 * Note: Does not need corresponding Router since FedQPLOperator is ours
 * and implements visit.
 */
public interface FedQPLVisitor<T> {
    default T visit(Mu mu) {return mu.visit(this);}
    default T visit(Mj mj) {return mj.visit(this);}
    default T visit(Req req) {return req.visit(this);}
    default T visit(LeftJoin lj) {return lj.visit(this);}
    default T visit(Filter filter) {return filter.visit(this);}
}
