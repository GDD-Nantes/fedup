package fr.gdd.fedqpl;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.OpCloningUtil;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.QueryUtils;
import org.apache.jena.sparql.util.VarUtils;

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
public class SA2FedQPL extends ReturningOpVisitor<List<Op>> {

    public static Op build(Op query, List<Map<Var, String>> assignments, ToQuadsTransform tqt){
        return new Mu(ReturningOpVisitorRouter.visit(new SA2FedQPL(assignments, tqt), query).stream().toList()); // root mu
    }

    public static Op build(Op query, List<Map<Var, String>> assignments, ToQuadsTransform tqt, Dataset assignmentsDataset){
        return new Mu(ReturningOpVisitorRouter.visit(new SA2FedQPL(assignments, tqt).setAssignmentsDataset(assignmentsDataset), query).stream().toList()); // root mu
    }

    /* *************************************************************** */

    List<Map<Var, String>> assignments;
    ToQuadsTransform toQuads;
    Dataset assignmentsDataset;

    public static boolean SILENT = true;

    // This is a way to save the work that has been done, but could
    // be returned instead.
    Map<Op, Map<Var, String>> fedQPL2PartialAssignment = new HashMap<>();

    public SA2FedQPL(List<Map<Var, String>> assignments, ToQuadsTransform tqt) {
        this.assignments = assignments;
        this.toQuads = tqt;
    }

    public SA2FedQPL setAssignmentsDataset(Dataset assignmentsDataset) { // TODO in constructor
        this.assignmentsDataset = assignmentsDataset;
        return this;
    }

    @Override
    public List<Op> visit(OpTriple opTriple) {
        Var g = toQuads.findVar(opTriple);
        MultiSet<Binding> bindings = this.sols(opTriple);
        return bindings.stream().map(b ->
                (Op) new OpService(b.get(g), opTriple, SILENT)
        ).toList();
    }

    @Override
    public List<Op> visit(OpBGP opBGP) {
        // Could do all possibilities by calling all sub-triple pattern
        // then examine which combinations actually checks out. But this would
        // be very inefficient.
        // Instead, checking directly which results exist
        Set<Var> gs = toQuads.findVars(opBGP);
        MultiSet<Binding> bindings = this.sols(opBGP);

        return bindings.stream().map(b -> {
            Mj mj = new Mj();
            for (Var g : gs) {
                OpTriple triple = new OpTriple(toQuads.getVar2quad().get(g).asTriple());
                Op req = new OpService(b.get(g), triple, SILENT);
                mj.addChild(req);
                toQuads.add(g, triple);
            }
            return (Op) mj;
        }).toList();
    }

    @Override
    public List<Op> visit(OpUnion union) {
        // nothing to register in `fedQPL2PartialAssignment`
        // since everything is already set on visit of left and right
        List<Op> results = new ArrayList<>();
        List<Op> lefts = ReturningOpVisitorRouter.visit(this, union.getLeft());
        List<Op> rights = ReturningOpVisitorRouter.visit(this, union.getRight());
        results.addAll(lefts);
        results.addAll(rights);
        return results;
    }

    @Override
    public List<Op> visit(OpJoin join) {
        List<Op> results = new ArrayList<>();

        // we want to examine each possibility once
        List<Op> lefts = new HashSet<>(ReturningOpVisitorRouter.visit(this, join.getLeft())).stream().toList();
        List<Op> rights = new HashSet<>(ReturningOpVisitorRouter.visit(this, join.getRight())).stream().toList();

        for (Op left : lefts) { // for each mandatory part
            for (Op right : rights) {
                if (this.ask(OpJoin.create(left, right))) {
                    results.add(new Mj(List.of(left, right)));
                }
            }
        }

        return results;
    }

    @Override
    public List<Op> visit(OpLeftJoin lj) {
        List<Op> results = new ArrayList<>();

        // we want to examine each possibility once
        List<Op> lefts = new HashSet<>(ReturningOpVisitorRouter.visit(this, lj.getLeft())).stream().toList();
        List<Op> rights = new HashSet<>(ReturningOpVisitorRouter.visit(this, lj.getRight())).stream().toList();

        for (Op left : lefts) { // for each mandatory part
            Mu mu = new Mu();

            for (Op right : rights) {
                if (this.ask(OpJoin.create(left, right))) {
                    mu.addChild(right);
                }
            }

            if (mu.getElements().isEmpty()) {
                results.add(left); // nothing in OPT
            } else if (mu.getElements().size() == 1) {
                OpLeftJoin leftJoin = OpCloningUtil.clone(lj, left, mu.get(0));
                results.add(leftJoin);
            } else {
                OpLeftJoin leftJoin = OpCloningUtil.clone(lj, left, mu);
                results.add(leftJoin);
            }
        }

        return results;
    }

    @Override
    public List<Op> visit(OpConditional cond) {
        return this.visit(OpLeftJoin.createLeftJoin(cond.getLeft(), cond.getRight(), ExprList.emptyList));
    }

    @Override
    public List<Op> visit(OpSlice slice) {
        // hijack the root, which will be mu(slice(mu(rest))) therefore
        // getting simplified easily
        return List.of(OpCloningUtil.clone(slice,
                new Mu(ReturningOpVisitorRouter.visit(this, slice.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpOrder orderBy) { // hijack too
        return List.of(OpCloningUtil.clone(orderBy,
                new Mu(ReturningOpVisitorRouter.visit(this, orderBy.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpProject project) { // hijack too
        System.out.println("meow");
        return List.of(OpCloningUtil.clone(project,
                new Mu(ReturningOpVisitorRouter.visit(this, project.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpDistinct distinct) {
        return List.of(OpCloningUtil.clone(distinct,
                new Mu(ReturningOpVisitorRouter.visit(this, distinct.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpFilter filter) {
        return List.of(OpCloningUtil.clone(filter,
                new Mu(ReturningOpVisitorRouter.visit(this, filter.getSubOp()).stream().toList())));
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


    public MultiSet<Binding> sols(Op op) {
        MultiSet<Binding> bindings = new HashMultiSet<>();
        boolean inTxn = this.assignmentsDataset.isInTransaction();
        if (!inTxn) this.assignmentsDataset.begin(ReadWrite.READ);

        Op checking = ReturningOpVisitorRouter.visit(new Op2SAChecker(this.toQuads), op);

        Plan plan = QueryEngineMain.getFactory().create(checking,
                assignmentsDataset.asDatasetGraph(),
                BindingRoot.create(),
                assignmentsDataset.getContext().copy());

        QueryIterator iterator = plan.iterator();

        while (iterator.hasNext()) {
            bindings.add(iterator.next());
        }

        if (!inTxn) {
            this.assignmentsDataset.commit();
            this.assignmentsDataset.end();
        }

        return bindings;
    }


    public boolean ask(Op op) {
        boolean inTxn = this.assignmentsDataset.isInTransaction();
        if (!inTxn) this.assignmentsDataset.begin(ReadWrite.READ);

        Op checking = ReturningOpVisitorRouter.visit(new Op2SAChecker(this.toQuads), op);

        Plan plan = QueryEngineMain.getFactory().create(checking,
                assignmentsDataset.asDatasetGraph(),
                BindingRoot.create(),
                assignmentsDataset.getContext().copy());

        QueryIterator iterator = plan.iterator();

        boolean result = iterator.hasNext();

        if (!inTxn) {
            this.assignmentsDataset.commit();
            this.assignmentsDataset.end();
        }

        return result;
    }

}
