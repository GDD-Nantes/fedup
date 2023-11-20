package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

/**
 * Basic operator of FedQPL that represents a logical SPARQL operator at
 * federation level.
 */
public interface FedQPLOperator {
    <T> T visit(FedQPLVisitor<T> visitor);
}
