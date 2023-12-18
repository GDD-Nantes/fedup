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
import org.apache.jena.sparql.algebra.optimize.VariableUsagePusher;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.engine.main.VarFinder;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.QueryUtils;
import org.apache.jena.sparql.util.VarUtils;
import org.eclipse.rdf4j.federated.optimizer.OptimizerUtil;

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

    public static Op build(Op query, ToQuadsTransform tqt, Dataset assignmentsDataset){
        SA2FedQPL builder = new SA2FedQPL(tqt, assignmentsDataset);
        List<Op> subExps = ReturningOpVisitorRouter.visit(builder, query);
        Mu rootUnion = new Mu(subExps.stream().toList());
        if (Objects.isNull(builder.topMostProjection)) {
            return OpCloningUtil.clone((OpProject) builder.createOpProject(query), rootUnion);
        } else {
            return rootUnion;
        }
    }

    /* *************************************************************** */

    ToQuadsTransform toQuads;
    Dataset assignmentsDataset;
    OpProject topMostProjection = null;

    public static boolean SILENT = true;

    public SA2FedQPL(ToQuadsTransform tqt, Dataset assignmentsDataset) {
        this.assignmentsDataset = assignmentsDataset;
        this.toQuads = tqt;
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
        if (Objects.isNull(this.topMostProjection)) {
            this.topMostProjection = project;
        }

        return List.of(OpCloningUtil.clone(project,
                new Mu(ReturningOpVisitorRouter.visit(this, project.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpDistinct distinct) {
        if (Objects.isNull(this.topMostProjection)) {
            return List.of(createOpProject(distinct.getSubOp()));
        }
        return List.of(OpCloningUtil.clone(distinct,
                new Mu(ReturningOpVisitorRouter.visit(this, distinct.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpFilter filter) {
        return List.of(OpCloningUtil.clone(filter,
                new Mu(ReturningOpVisitorRouter.visit(this, filter.getSubOp()).stream().toList())));
    }

    /* *************************************************************** */

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

    /* ************************************************************* */

    public Op createOpProject(Op query) {
            VarFinder vars = VarFinder.process(query);
            Set<Var> allVariables = new HashSet<>();
            allVariables.addAll(vars.getAssign());
            allVariables.addAll(vars.getFilter());
            allVariables.addAll(vars.getFixed());
            allVariables.addAll(vars.getOpt());
            allVariables.addAll(vars.getFilterOnly());
            this.topMostProjection = new OpProject(new Mu(ReturningOpVisitorRouter.visit(this, query)),
                    allVariables.stream().toList());
            return this.topMostProjection;
        }
    }
