package fr.gdd.fedup.summary;

import fr.gdd.fedup.transforms.AddFilterForAskedGraphs;
import org.apache.jena.graph.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.core.Var;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Good old hashing on URI suffix.
 */
public class ModuloOnSuffix extends LeavePredicateUntouched {

    Integer modulo;
    AddFilterForAskedGraphs affag;

    public ModuloOnSuffix(Integer modulo) {
        this.modulo = modulo;
    }

    public void setAffag(AddFilterForAskedGraphs affag) {
        this.affag = affag;
    }

    public Node transform(Node node) {
        return switch (node) {
            case Node_URI ignored -> {
                try {
                    URI uri = new URI(node.getURI());
                    int hashcode = Math.abs(uri.toString().hashCode());
                    if (modulo == 0 || modulo == 1) {
                        yield NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost());
                    } else {
                        yield NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost() + "/" + (hashcode % modulo));
                    }
                } catch (URISyntaxException e) {
                    yield NodeFactory.createURI("https://donotcare.com/whatever");
                }
            }
            case Node_Blank ignored -> NodeFactory.createBlankNode("_:any");
            case Node_Literal ignored -> NodeFactory.createLiteralString("any");
            default -> Var.alloc(node.getName());
        };
    }

    /* ************************************************************************* */

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        if (!affag.askFilters.contains(opFilter.getExprs())) {
            return subOp; // TODO: handle special filter expressions, i.e., we don't want to remove simple equalities
        }
        return opFilter;
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        if (!affag.askFilters.contains(opLeftJoin.getExprs())) {
            return new OpConditional(left, right);
        }
        return opLeftJoin;
    }
}
