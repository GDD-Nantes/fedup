package fr.gdd.fedqpl;

import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Objects;

/**
 * Never better served than by yourselves. TODO TODO TODO TODO
 */
public class SPARQL2String extends ReturningOpVisitor<String> {

    @Override
    public String visit(OpConditional cond) {
        return String.format("{%s} OPTIONAL {%s}",
                ReturningOpVisitorRouter.visit(this, cond.getLeft()),
                ReturningOpVisitorRouter.visit(this, cond.getRight()));
    }

    @Override
    public String visit(OpJoin join) {
        return String.format("{%s} JOIN {%s}",
                ReturningOpVisitorRouter.visit(this, join.getLeft()),
                ReturningOpVisitorRouter.visit(this, join.getRight()));
    }

    @Override
    public String visit(OpLeftJoin lj) {
        if (Objects.nonNull(lj.getExprs()) && !lj.getExprs().isEmpty()) {
            return String.format("{%s} OPTIONAL {%s}",
                    ReturningOpVisitorRouter.visit(this, lj.getLeft()),
                    ReturningOpVisitorRouter.visit(this, lj.getRight()));
        } else {
            return String.format("{%s} OPTIONAL {%s FILTER (%s)}",
                    ReturningOpVisitorRouter.visit(this, lj.getLeft()),
                    ReturningOpVisitorRouter.visit(this, lj.getRight()),
                    String.join("&&",
                            lj.getExprs().getList().stream().map(ExprUtils::fmtSPARQL).toList()));
        }
    }

    @Override
    public String visit(OpSequence sequence) {
        String inside = String.join("} JOIN {",
                sequence.getElements().stream().map(s-> ReturningOpVisitorRouter.visit(this, s)).toList());
        return "{" + inside + "}";
    }

    @Override
    public String visit(OpOrder orderBy) {
        return String.format("%s ORDER BY (%s)",
                ReturningOpVisitorRouter.visit(this, orderBy.getSubOp()),
                String.join("&&",
                        orderBy.getConditions().stream().map(sc -> ExprUtils.fmtSPARQL(sc.getExpression())).toList()));
    }

    @Override
    public String visit(OpFilter filter) {
        return String.format("{%s} FILTER (%s)",
                ReturningOpVisitorRouter.visit(this, filter.getSubOp()),
                String.join("&&",
                        filter.getExprs().getList().stream().map(ExprUtils::fmtSPARQL).toList()));
    }

    @Override
    public String visit(OpSlice slice) {
        String result = ReturningOpVisitorRouter.visit(this, slice.getSubOp());
        if (slice.getStart() > 0) {
            result += " OFFSET " + slice.getStart();
        }
        if (slice.getLength() > 0) {
            result += " LIMIT " + slice.getLength();
        }
        return result;
    }

}
