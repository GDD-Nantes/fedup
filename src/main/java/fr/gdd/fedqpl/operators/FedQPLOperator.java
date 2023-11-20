package fr.gdd.fedqpl.operators;

import fr.gdd.fedqpl.visitors.FedQPLVisitor;

/**
 * Basic operator of FedQPL.
 */
public interface FedQPLOperator {
    <T> T visit(FedQPLVisitor<T> visitor);
}
