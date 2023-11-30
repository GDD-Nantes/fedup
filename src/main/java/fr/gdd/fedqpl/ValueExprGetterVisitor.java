package fr.gdd.fedqpl;

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractSimpleQueryModelVisitor;

public class ValueExprGetterVisitor extends AbstractSimpleQueryModelVisitor<RuntimeException> {

    @Override
    public void meet(Filter node)  {
        throw new ValueExprException(node.getCondition());
    }
}
