package fr.gdd.fedqpl.visitors;

import org.apache.jena.sparql.algebra.OpVisitorBase;

import fr.gdd.fedqpl.operators.Filter;
import fr.gdd.fedqpl.operators.LeftJoin;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;

/**
 * A visitor of FedQPL expressions with a return type that can
 * be parametrized if need be, in a functional way.
 */
public abstract class FedQPLVisitor extends OpVisitorBase {

    public void visit(Mu mu) {};

    public void visit(Mj mj) {};

    public void visit(Req req) {};

    public void visit(LeftJoin lj) {};

    public void visit(Filter filter) {};

}
