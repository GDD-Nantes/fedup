package fr.gdd.fedup.summary;

import org.apache.jena.graph.*;
import org.apache.jena.sparql.core.Var;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Good old hashing on URI suffix.
 */
public class TruncateOnSuffix extends LeavePredicateUntouched {

    Integer truncLength = 3;

    public TruncateOnSuffix(){}
    public TruncateOnSuffix(Integer truncLength) {
        this.truncLength = truncLength;
    }

    public Node transform(Node node) {
        return switch (node) {
            case Node_URI ignored -> {
                try {
                    URI uri = new URI(node.getURI());

                    String truncated = uri.getScheme() + "://" + uri.getHost();
                    String rest = uri.toString().replace(truncated, "");

                    if(Objects.nonNull(uri.getPath())) {
                         truncated = truncated + rest.substring(0, Math.min(uri.getPath().length(), truncLength));
                    }

                    Node summarized = NodeFactory.createURI(truncated);

                    yield summarized;
                } catch (URISyntaxException e) {
                    yield NodeFactory.createURI("https://donotcare.com/whatever");
                }
            }
            case Node_Blank ignored -> NodeFactory.createBlankNode("_:any");
            case Node_Literal ignored -> NodeFactory.createLiteralString("any");
            default -> Var.alloc(node.getName());
        };
    }

    protected Node processURI(URI uri) {
        String truncated = uri.getScheme() + "://" + uri.getHost();
        String rest = uri.toString().replace(truncated, "");

        if(Objects.nonNull(uri.getPath())) {
            truncated = truncated + rest.substring(0, Math.min(uri.getPath().length(), truncLength));
        }

        Node summarized = NodeFactory.createURI(truncated);

        return summarized;
    }
}
