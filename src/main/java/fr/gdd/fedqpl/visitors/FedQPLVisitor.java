package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.LeftJoin;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;

/**
 * A visitor of FedQPL expressions with a return type that can
 * be parametrized if need be, in a functional way.
 * @param <T> The return type.
 */
public class FedQPLVisitor<T, U> {

    public T visit(Mu mu, U args) { throw new UnsupportedOperationException(); }

    public T visit(Mj mj, U args) { throw new UnsupportedOperationException(); }

    public T visit(Req req, U args) { throw new UnsupportedOperationException(); }

    public T visit(LeftJoin lj, U args) { throw new UnsupportedOperationException(); }

}
