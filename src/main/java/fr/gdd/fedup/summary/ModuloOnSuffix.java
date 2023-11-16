package fr.gdd.fedup.summary;

import fr.gdd.fedup.transforms.AddFilterForAskedGraphs;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
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

    Integer modulo = 1;
    AddFilterForAskedGraphs affag;

    public ModuloOnSuffix(Integer modulo) {
        this.modulo = modulo;
    }

    public void setAffag(AddFilterForAskedGraphs affag) {
        this.affag = affag;
    }

    public Node transform(Node node) {
        if (node.isURI()) {
            try {
                URI uri = new URI(node.getURI());
                int hashcode = Math.abs(uri.toString().hashCode());
                if (modulo == 0 || modulo == 1) {
                    return NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost());
                } else {
                    return NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost() + "/" + (hashcode % modulo));
                }
            } catch (URISyntaxException e) {
                return NodeFactory.createURI("https://donotcare.com/whatever");
            }
        } else if (node.isLiteral()) {
            return NodeFactory.createLiteral("any");
        } else {
            return Var.alloc(node.getName());
        }
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
