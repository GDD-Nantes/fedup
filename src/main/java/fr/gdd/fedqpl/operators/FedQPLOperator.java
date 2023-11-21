package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;
import fr.gdd.fedqpl.visitors.FedQPLVisitorArg;

/**
 * Basic operator of FedQPL that represents a logical SPARQL operator at
 * federation level.
 */
public interface FedQPLOperator {
    <T> T visit(FedQPLVisitor<T> visitor);
    <T, S> T visit(FedQPLVisitorArg<T, S> visitor, S arg);
}
