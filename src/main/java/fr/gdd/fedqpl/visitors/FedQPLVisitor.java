package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import org.apache.jena.sparql.algebra.op.*;

/**
 * A visitor of FedQPL expressions with a return type that can
 * be parametrized if need be, in a functional way.
 * Note: Does not need corresponding Router since FedQPLOperator is ours
 * and implements visit.
 */
public interface FedQPLVisitor<T> {
    // operators
    default T visit(Mu mu) {throw new UnsupportedOperationException("Mu");}
    default T visit(Mj mj) {throw new UnsupportedOperationException("Mj");}
    default T visit(OpService req) {throw new UnsupportedOperationException("Req");}
    default T visit(OpConditional lj) {throw new UnsupportedOperationException("LeftJoin");}
    default T visit(OpFilter filter) {throw new UnsupportedOperationException("Filter");}

    // query modifiers
    default T visit(OpSlice limit) {throw new UnsupportedOperationException("Limit");}
    default T visit(OpOrder orderBy) {throw new UnsupportedOperationException("OrderBy");}
    default T visit(OpProject project) {throw new UnsupportedOperationException("Project");}
}

