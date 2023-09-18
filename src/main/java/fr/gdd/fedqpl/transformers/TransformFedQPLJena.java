package fr.gdd.fedqpl.transformers;

import fr.gdd.fedqpl.operators.*;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

import java.util.Iterator;

/**
 * Transforms a FedQPL expression into a Jena operator so Jena's engine
 * can execute it.
 */
public class TransformFedQPLJena extends TransformCopy {

    public TransformFedQPLJena() {}

    public Op transform(Mu mu) {
        return switch (mu.getChildren().size()) {
            case 0 -> OpNull.create();
            case 1 -> Transformer.transform(this, mu.getChildren().iterator().next());
            default -> {
                Iterator<FedQPLOperator> ops = mu.getChildren().iterator();
                Op left = ops.next();
                while (ops.hasNext()) {
                    Op right = ops.next();
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

    public Op transform(Mj mj) {
        return switch (mj.getChildren().size()) {
            case 0 -> OpNull.create();
            case 1 -> Transformer.transform(this, mj.getChildren().iterator().next());
            default -> {
                Iterator<FedQPLOperator> ops = mj.getChildren().iterator();
                Op left = ops.next();
                while (ops.hasNext()) {
                    Op right = ops.next();
                    left = OpJoin.create(left, right);
                }
                yield left;
            }
        };
    }

    public Op transform(Req opReq) {
        return new OpQuad(new Quad(opReq.getSource(), opReq.getTriple()));
    }

    public Op transform(Filter opFilter) {
        return OpFilter.filterBy(
            opFilter.getExprs(), 
            Transformer.transform(this, opFilter.getSubOp())
        );
    }

    public Op transform(LeftJoin opLeftJoin) {
        return OpJoin.create(
            Transformer.transform(this, opLeftJoin.getLeft()), 
            Transformer.transform(this, opLeftJoin.getRight())
        );
    }

    /**
     * Build SPARQL representation with a graph clause
     * @param op the root of FedQPL expression
     * @return an intermediate SPARQL representation
     */
    public Query buildQuery(FedQPLOperator op) {
        Op jenaOp = Transformer.transform(this, op);
        String graphVarName = "?g" + Long.toString(System.currentTimeMillis());
        OpGraph opGraph = new OpGraph(Var.alloc(graphVarName), jenaOp);
        Query query = OpAsQuery.asQuery(opGraph);
        query.setQuerySelectType();

        return query;
    }

    public boolean hasSolutions(FedQPLOperator op, Dataset dataset) {
        Query query = buildQuery(op);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        ResultSet results = qexec.execSelect();
        return results.hasNext();

    }

}
