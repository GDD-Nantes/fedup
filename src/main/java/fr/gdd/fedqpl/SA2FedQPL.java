package fr.gdd.fedqpl;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.OpCloningUtil;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.commons.collections4.MultiSet;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.VarFinder;
import org.apache.jena.sparql.expr.ExprList;

import java.util.*;

/**
 * A source assignments is a list of sources that are expected to provide
 * actual results. By itself, it is not sufficient to provide correct and
 * complete results of the query.
 *
 * Along with the original query plan, this visitor converts the source assignments
 * into a FedQPL expression that encodes the federated query to perform.
 */
public class SA2FedQPL extends ReturningOpVisitor<List<Op>> {

    public static Op build(Op query, ToQuadsTransform tqt, SAAsKG assignmentsAsKG){
        SA2FedQPL builder = new SA2FedQPL(tqt, assignmentsAsKG);
        List<Op> subExps = ReturningOpVisitorRouter.visit(builder, query);
        Mu rootUnion = new Mu(subExps.stream().toList());

        if (Objects.isNull(builder.topMostProjection)) {
            return OpCloningUtil.clone(builder.createOpProjectWithAllVariables(query), rootUnion);
        } else {
            return rootUnion;
        }
    }

    /* *************************************************************** */

    final ToQuadsTransform toQuads;
    final SAAsKG assignmentsAsKG;

    OpProject topMostProjection = null;

    public static boolean SILENT = true;

    public SA2FedQPL(ToQuadsTransform tqt, SAAsKG assignmentsAsKG) {
        this.assignmentsAsKG = assignmentsAsKG;
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
        if (Objects.isNull(this.topMostProjection)) {this.topMostProjection = project;}

        return List.of(OpCloningUtil.clone(project,
                new Mu(ReturningOpVisitorRouter.visit(this, project.getSubOp()).stream().toList())));
    }

    @Override
    public List<Op> visit(OpDistinct distinct) {
        Op below = new Mu(ReturningOpVisitorRouter.visit(this, distinct.getSubOp()));

        if (!(distinct.getSubOp() instanceof OpProject)) {
            // if there is no project below distinct, we add it.
            this.topMostProjection = createOpProjectWithAllVariables(distinct.getSubOp());
            below = OpCloningUtil.clone(this.topMostProjection, below);
        }

        return List.of(OpCloningUtil.clone(distinct, below));
    }

    @Override
    public List<Op> visit(OpFilter filter) {
        return List.of(OpCloningUtil.clone(filter,
                new Mu(ReturningOpVisitorRouter.visit(this, filter.getSubOp()).stream().toList())));
    }

    /* *************************************************************** */

    public MultiSet<Binding> sols(Op op) {
        return assignmentsAsKG.sols(op);
    }

    public boolean ask(Op op) {
        return assignmentsAsKG.ask(op);
    }

    /* ************************************************************* */

    public static OpProject createOpProjectWithAllVariables(Op query) {
            VarFinder vars = VarFinder.process(query);
            Set<Var> allVariables = new HashSet<>();
            allVariables.addAll(vars.getAssign());
            allVariables.addAll(vars.getFilter());
            allVariables.addAll(vars.getFixed());
            allVariables.addAll(vars.getOpt());
            allVariables.addAll(vars.getFilterOnly());
            return new OpProject(null, allVariables.stream().toList());
        }
    }
