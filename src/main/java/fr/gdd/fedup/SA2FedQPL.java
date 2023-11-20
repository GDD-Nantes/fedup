package fr.gdd.fedup;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Convert source assignments & logical plan into a service query.
 */
public class SA2FedQPL extends ReturningOpVisitor<FedQPLOperator> {

    public static FedQPLOperator build(Op query, List<Map<Var, String>> assignments, ToQuadsTransform tqt){
        Mu mu = new Mu();
        assignments.forEach(a -> {
            SA2FedQPL sa2sq = new SA2FedQPL(a, tqt);
            mu.addChild(ReturningOpVisitorRouter.visit(sa2sq, query));
        });
        return mu;
    }

    Map<Var, String> assignment;
    ToQuadsTransform toQuads;

    public SA2FedQPL(Map<Var, String> assignment, ToQuadsTransform tqt) {
        this.assignment = assignment;
        this.toQuads = tqt;
    }

    @Override
    public FedQPLOperator visit(OpTriple opTriple) {
        Quad quad = null;
        for (Var g : assignment.keySet()) {
            if (toQuads.getQuad2var().containsKey(new Quad(g, opTriple.getTriple()))) {
                quad = new Quad(g, opTriple.getTriple());
                break;
            }
        }
        return Objects.isNull(quad)?
                null:
                new Req(opTriple, NodeFactory.createURI(assignment.get(quad.getGraph())));
    }

    @Override
    public FedQPLOperator visit(OpBGP opBGP) {
        Mj mj = new Mj();
        for (Triple t : opBGP.getPattern().getList()) {
            mj.addChild(this.visit(new OpTriple(t)));
        }
        return mj;
    }
}
