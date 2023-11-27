package fr.gdd.fedqpl;

import fr.gdd.fedqpl.operators.*;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A source assignments is a list of sources that are expected to provide
 * actual results. By itself, it is not sufficient to provide correct and
 * complete results of the query.
 *
 * Along with the original query plan, this visitor converts the source assignments
 * into a FedQPL expression that encodes the federated query to perform.
 */
public class SA2FedQPL extends ReturningOpVisitor<Set<Op>> {

    public static Op build(Op query, List<Map<Var, String>> assignments, ToQuadsTransform tqt){
        return new Mu(ReturningOpVisitorRouter.visit(new SA2FedQPL(assignments, tqt), query).stream().toList()); // root mu
    }

    /* *************************************************************** */

    List<Map<Var, String>> assignments;
    ToQuadsTransform toQuads;

    public static boolean SILENT = true;

    // This is a way to save the work that has been done, but could
    // be returned instead.
    Map<Op, Map<Var, String>> fedQPL2PartialAssignment = new HashMap<>();

    public SA2FedQPL(List<Map<Var, String>> assignments, ToQuadsTransform tqt) {
        this.assignments = assignments;
        this.toQuads = tqt;
    }

    @Override
    public Set<Op> visit(OpTriple opTriple) {
        Set<Op> result = new HashSet<>();
        Var g = toQuads.findVar(opTriple);
        for (Map<Var, String> assignment : assignments) {
            if (assignment.containsKey(g)) {
                OpService req = new OpService(NodeFactory.createURI(assignment.get(g)), opTriple, SILENT);
                result.add(req);
                fedQPL2PartialAssignment.put(req, Map.of(g, assignment.get(g)));
            }
        }
        return result;
    }

    @Override
    public Set<Op> visit(OpBGP opBGP) {
        // Could do all possibilities by calling all sub-triple pattern
        // then examine which combinations actually checks out. But this would
        // be very inefficient.
        // Instead, checking directly which results exist
        Set<Op> result = new HashSet<>();

        Set<Var> gs = toQuads.findVars(opBGP);

        List<Map<Var, String>> saOfGs = allSourcesAreSet(gs);

        for (Map<Var, String> assignment : saOfGs) {
            Mj mj = new Mj();
            assignment.entrySet().forEach(a -> {
                OpService req = new OpService(NodeFactory.createURI(a.getValue()),
                        new OpTriple(toQuads.getVar2quad().get(a.getKey()).asTriple()),
                        SILENT
                        );
                mj.addChild(req);
            });
            result.add(mj);
            fedQPL2PartialAssignment.put(mj, assignment);
        }

        return result;
    }

    @Override
    public Set<Op> visit(OpUnion union) {
        // nothing to register in `fedQPL2PartialAssignment`
        // since everything is already set on visit of left and right
        Set<Op> results = new HashSet<>();
        Set<Op> lefts = ReturningOpVisitorRouter.visit(this, union.getLeft());
        Set<Op> rights = ReturningOpVisitorRouter.visit(this, union.getRight());
        results.addAll(lefts);
        results.addAll(rights);
        return results;
    }

    @Override
    public Set<Op> visit(OpLeftJoin lj) {
        Set<Op> results = new HashSet<>();

        Set<Op> lefts = ReturningOpVisitorRouter.visit(this, lj.getLeft());
        Set<Op> rights = ReturningOpVisitorRouter.visit(this, lj.getRight());

        for (Op left : lefts) { // for each mandatory part
            Mu mu = new Mu();

            for (Op right : rights) {
                Map<Var, String> assignmentToTest = new HashMap<>();
                assignmentToTest.putAll(fedQPL2PartialAssignment.get(left));
                assignmentToTest.putAll(fedQPL2PartialAssignment.get(right));
                if (theResultExists(assignmentToTest)) {
                    mu.addChild(right);
                    fedQPL2PartialAssignment.put(mu, assignmentToTest);
                }
            }

            if (mu.getElements().isEmpty()) {
                results.add(left); // nothing in OPT
            } else {
                OpConditional leftJoin = new OpConditional(left, mu);
                results.add(leftJoin);
                Map<Var, String> assignmentToAdd = new HashMap<>();
                assignmentToAdd.putAll(fedQPL2PartialAssignment.get(left));
                assignmentToAdd.putAll(fedQPL2PartialAssignment.get(mu));
                fedQPL2PartialAssignment.put(leftJoin, assignmentToAdd);
            }
        }

        return results;
    }

    @Override
    public Set<Op> visit(OpSlice slice) {
        // hijack the root, which will be mu(slice(mu(rest))) therefore
        // getting simplified easily
        return Set.of(new OpSlice(
                new Mu(ReturningOpVisitorRouter.visit(this, slice.getSubOp()).stream().toList()),
                slice.getStart(),
                slice.getLength()));
    }

    @Override
    public Set<Op> visit(OpOrder orderBy) {
        // hijack the root, which will be mu(slice(mu(rest))) therefore
        // getting simplified easily
        return Set.of(new OpOrder(
                new Mu(ReturningOpVisitorRouter.visit(this, orderBy.getSubOp()).stream().toList()),
                orderBy.getConditions()));
    }

    @Override
    public Set<Op> visit(OpProject project) {
        // hijack the root, which will be mu(slice(mu(rest))) therefore
        // getting simplified easily
        return Set.of(new OpProject(new Mu(ReturningOpVisitorRouter.visit(this, project.getSubOp()).stream().toList()),
                project.getVars()));
    }

    @Override
    public Set<Op> visit(OpDistinct distinct) {
        return Set.of(new OpDistinct(new Mu(ReturningOpVisitorRouter.visit(this, distinct.getSubOp()).stream().toList())));
    }

    /* *************************************************************** */

    /**
     * @param vars The set of variables to check.
     * @return All results where the variables are set along with their value.
     */
    public List<Map<Var, String>> allSourcesAreSet(Set<Var> vars) {
        List<Map<Var, String>> results = new ArrayList<>();
        for (Map<Var, String> assignment : assignments) {
            if (vars.stream().allMatch(assignment::containsKey)) {
                // only keep Var that are in vars
                Map<Var, String> cutAssignment = assignment.entrySet().stream().filter(e -> vars.contains(e.getKey()) ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                results.add(cutAssignment);
            }
        }
        return results;
    }

    /**
     * @param partialAssignment Variables and their respective value.
     * @return True if in an assignment, they all exist and are set with the appropriate value
     */
    public boolean theResultExists(Map<Var, String> partialAssignment) {
        for (Map<Var, String> assignment : assignments) {
            if (partialAssignment.entrySet().stream()
                    .allMatch(e -> assignment.containsKey(e.getKey()) && assignment.get(e.getKey()).equals(e.getValue()))) {
                return true;
            }
        }
        return false;
    }

}
