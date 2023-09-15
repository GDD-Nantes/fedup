package fr.gdd.fedqpl.transformers;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Filter;
import fr.gdd.fedqpl.operators.LeftJoin;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Var;

public class TransformFedQPLJena extends TransformCopy {
    Query query;

    public TransformFedQPLJena() {
        query = QueryFactory.create();
    }

    public Op transform(Mu opMu) {
        List<FedQPLOperator> children = opMu.getChildren().stream().collect(Collectors.toList());

        if (children.size() == 1) {
            return Transformer.transform(this, children.get(0));
        } else if (children.size() > 1) {
            OpUnion opUnion = new OpUnion(
                    Transformer.transform(this, children.get(0)),
                    Transformer.transform(this, children.get(1)));

            for (int i = 2; i < children.size(); i++) {
                opUnion = new OpUnion(opUnion, Transformer.transform(this, children.get(i)));
            }
            return opUnion;
        } else {
            throw new IllegalArgumentException("Could not construct OpUnion from Mu with no child!");
        }

    }

    public Op transform(Mj opMj) {
        List<FedQPLOperator> children = opMj.getChildren().stream().collect(Collectors.toList());

        if (children.size() == 1) {
            return Transformer.transform(this, children.get(0));
        } else if (children.size() > 1) {
            Op opJoin = OpJoin.create(
                    Transformer.transform(this, children.get(0)),
                    Transformer.transform(this, children.get(1)));
            
            for (int i = 2; i < children.size(); i++) {
                opJoin = OpJoin.create(opJoin, Transformer.transform(this, children.get(i)));
            }
            return opJoin;
        } else {
            throw new IllegalArgumentException("Could not construct OpUnion from Mj with no child!");
        }

    }

    public Op transform(Req opReq) {
        OpTriple opTriple = new OpTriple(opReq.getTriple());
        return opTriple;
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
