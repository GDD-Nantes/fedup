package fr.gdd.fedup.source.selection;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import org.apache.jena.sparql.algebra.Op;

import java.util.Iterator;
import java.util.Objects;

/**
 * Creates Apache Jena queries depending on the federated plan.
 * TODO create multiple RAW in different threads. (Even though current Jena's iterator as parallel as they should be.
 * TODO add optional optimizer, i.e., query decomposition algorithms, cache optimizer etc.
 */
public class FedQPL2SparqlIterator implements Iterator<Op> {

    RWsToFedQPLIterator raws;

    FedQPLOperator pending;
    FedQPLOperator alreadyPushed;

    public void FedQPL2SparqlIterator(RWsToFedQPLIterator raws) {
        this.raws = raws;
    }

    @Override
    public boolean hasNext() {
        while (raws.hasNext() && Objects.isNull(pending)) {
            FedQPLOperator next = raws.next();
            // TODO #1 next = next setminus alreadyPushed
            // TODO #2 pending.merge(next)
        }
        return Objects.nonNull(pending);
    }

    @Override
    public Op next() {
        // TODO get a branch from pending
        // TODO remove this branch from pending
        // TODO merge this branch to alreadyPushed
        // TODO return the branch
        return null;
    }
}
