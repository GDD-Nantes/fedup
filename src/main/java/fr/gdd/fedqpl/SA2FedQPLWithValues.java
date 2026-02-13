package fr.gdd.fedqpl;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.visitors.OpCloningUtil;
import fr.gdd.fedqpl.visitors.ReturningOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.SetUtils;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.table.TableBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.main.VarFinder;
import org.apache.jena.sparql.expr.ExprList;

import java.util.*;
import java.util.stream.Collectors;

import static fr.gdd.fedqpl.SA2FedQPLWithValues.VisitorRecord;

/**
 * A source assignments is a list of sources that are expected to provide
 * actual results. By itself, it is not sufficient to provide correct and
 * complete results of the query.
 *
 * Along with the original query plan, this visitor converts the source assignments
 * into a FedQPL expression that encodes the federated query to perform.
 */
public class SA2FedQPLWithValues extends ReturningOpVisitor<VisitorRecord> {

    public static Op build(Op query, ToQuadsTransform tqt, SAAsKG assignmentsAsKG){
        SA2FedQPLWithValues builder = new SA2FedQPLWithValues(tqt, assignmentsAsKG);

        VisitorRecord queryAndSourceCombinations;

        assignmentsAsKG.dataset.begin(TxnType.READ);
        try {
            queryAndSourceCombinations = ReturningOpVisitorRouter.visit(builder, query);
        } catch (Exception e) {
            assignmentsAsKG.dataset.end();
            throw e;
        }
        assignmentsAsKG.dataset.end();

        Op queryWithServices = queryAndSourceCombinations.withServices;
        List<Binding> sourceCombinations = queryAndSourceCombinations.bindings;

        Op valuesServiceQuery = createValuesServiceQuery(queryWithServices, sourceCombinations);

        if (Objects.isNull(builder.topMostProjection)) {
            return OpCloningUtil.clone(builder.createOpProjectWithAllVariables(query), valuesServiceQuery);
        } else {
            return valuesServiceQuery;
        }
    }

    public static Op createValuesServiceQuery(Op serviceQuery, List<Binding> bindings) {
        if(bindings.isEmpty()) return OpTable.empty();
        if(bindings.size() == 1 && bindings.getFirst().isEmpty()) return serviceQuery;

        TableBuilder tableBuilder = TableFactory.builder();
        bindings.stream().forEach(tableBuilder::addRow);
        OpTable opTable = OpTable.create(tableBuilder.build());

        return OpSequence.create(opTable, serviceQuery);
    }

    /* *************************************************************** */

    final ToQuadsTransform toQuadsTransform;
    final SAAsKG assignmentsAsKG;

    OpProject topMostProjection = null;

    public static boolean SILENT = true;

    public SA2FedQPLWithValues(ToQuadsTransform tqt, SAAsKG assignmentsAsKG) {
        this.assignmentsAsKG = assignmentsAsKG;
        this.toQuadsTransform = tqt;
    }

    @Override
    public VisitorRecord visit(OpTriple opTriple) {
        MultiSet<Binding> bindings = this.sols(opTriple);
        Var g = toQuadsTransform.findVar(opTriple);
        return new VisitorRecord(
                new OpService(g, opTriple, false),
                new OpQuad(Quad.create(g, opTriple.getTriple())),
                new ArrayList<>(
                    bindings.uniqueSet().stream()
                        .map(b -> BindingBuilder.create().add(g, b.get(g)).build())
                        .collect(Collectors.toList())),
                Set.of(g));
    }

    @Override
    public VisitorRecord visit(OpBGP opBGP) {
        // Could do all possibilities by calling all sub-triple pattern
        // then examine which combinations actually checks out. But this would
        // be very inefficient.
        // Instead, checking directly which results exist
        MultiSet<Binding> bindings = this.sols(opBGP);
        Set<Var> vars = toQuadsTransform.findVars(opBGP);

        Mj servicesMj = new Mj();
        Mj grapshMj = new Mj();

        for (Var var : vars) {
            Quad quad = toQuadsTransform.getVar2quad().get(var);
            servicesMj.add(new OpService(var, new OpTriple(quad.asTriple()), false));
            grapshMj.add(new OpQuad(quad));
        }

        return new VisitorRecord(
                servicesMj,
                grapshMj,
                new ArrayList<>(bindings.uniqueSet().stream()
                    .map(b -> {
                            // Restricting bindings to the graph / service variables
                            BindingBuilder bb = Binding.builder();
                            for (Var g : vars) {
                                bb.add(g, b.get(g));
                            }
                            return bb.build();
                    }).collect(Collectors.toList())),
                vars);
    }

    @Override
    public VisitorRecord visit(OpUnion union) {
        // nothing to register in `fedQPL2PartialAssignment`
        // since everything is already set on visit of left and right
        VisitorRecord left = ReturningOpVisitorRouter.visit(this, union.getLeft());
        VisitorRecord right = ReturningOpVisitorRouter.visit(this, union.getRight());

        Set<Var> varsSeen = SetUtils.union(left.varsSeen, right.varsSeen);

        Op leftBranch = createValuesServiceQuery(left.withServices, left.bindings);
        Op rightBranch = createValuesServiceQuery(right.withServices, right.bindings);

        return new VisitorRecord(
                OpUnion.create(leftBranch, rightBranch),
                OpUnion.create(left.withGraphs, right.withGraphs),
                List.of(Binding.builder().build()),
                varsSeen
        );
    }

    @Override
    public VisitorRecord visit(OpJoin join) {
        List<Binding> results = new ArrayList<>();

        VisitorRecord left = ReturningOpVisitorRouter.visit(this, join.getLeft());
        VisitorRecord right = ReturningOpVisitorRouter.visit(this, join.getRight());

        Op servicesJoin = OpJoin.create(left.withServices, right.withServices);


        // we want to examine each possibility once
        for (Binding leftBinding : new HashSet<>(left.bindings)) { // for each mandatory part
            for (Binding rightBinding : new HashSet<>(right.bindings)) {
                Op instantiated = Substitute.inject(Substitute.inject(servicesJoin, rightBinding), leftBinding);
                if (this.ask(instantiated)) {
                    results.add(Binding.builder().addAll(leftBinding).addAll(rightBinding).build());
                }
            }
        }

        return new VisitorRecord(
                OpJoin.create(left.withServices, right.withServices),
                OpJoin.create(left.withGraphs, right.withGraphs),
                results,
                SetUtils.union(left.varsSeen, right.varsSeen)
        );
    }

    @Override
    public VisitorRecord visit(OpLeftJoin lj) {
        throw new UnsupportedOperationException("Not supported yet.");
//        List<Binding> results = new ArrayList<>();
//
//        // We want to examine each possibility once
//        List<Binding> lefts = new HashSet<>(ReturningOpVisitorRouter.visit(this, lj.getLeft())).stream().toList();
//        List<Binding> rights = new HashSet<>(ReturningOpVisitorRouter.visit(this, lj.getRight())).stream().toList();
//
//        for (Binding left : lefts) { // for each mandatory part
//            List<Binding> rightsMatched = new ArrayList<>();
//            for (Binding right : rights) {
//                Op instantiated = Substitute.inject(Substitute.inject(lj, right), left);
//                if (this.ask(instantiated)) {
//                    rightsMatched.add(Binding.builder().addAll(left).addAll(right).build());
//
//                }
//            }
//
//
//            if (rightsMatched.isEmpty()) {
//                results.add(left); // nothing in OPT
//            } else {
//                for(Binding rightMatched : rightsMatched) {
//                    results.add(Binding.builder().addAll(left).addAll(rightMatched).build());
//                }
//            }
//        }
//
//        return results;
    }

    @Override
    public VisitorRecord visit(OpConditional cond) {
        return this.visit(OpLeftJoin.createLeftJoin(cond.getLeft(), cond.getRight(), ExprList.emptyList));
    }

    @Override
    public VisitorRecord visit(OpSlice slice) {
        // hijack the root, which will be mu(slice(mu(rest))) therefore
        // getting simplified easily
        return ReturningOpVisitorRouter.visit(this, slice.getSubOp());
    }

    @Override
    public VisitorRecord visit(OpOrder orderBy) { // hijack too
        return ReturningOpVisitorRouter.visit(this, orderBy.getSubOp());
    }

    @Override
    public VisitorRecord visit(OpProject project) { // hijack too
        if (Objects.isNull(this.topMostProjection)) {this.topMostProjection = project;}

        return ReturningOpVisitorRouter.visit(this, project.getSubOp());
    }

    @Override
    public VisitorRecord visit(OpDistinct distinct) {
        return ReturningOpVisitorRouter.visit(this, distinct.getSubOp());
    }

    @Override
    public VisitorRecord visit(OpFilter filter) {
        return ReturningOpVisitorRouter.visit(this, filter.getSubOp());
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

    public static class VisitorRecord {
        Op withServices;
        Op withGraphs;
        List<Binding> bindings;
        Set<Var> varsSeen = new HashSet<>();

        public VisitorRecord(Op withServices, Op withGraphs, List<Binding> bindings, Set<Var> varsSeen) {
            this.withServices = withServices;
            this.withGraphs = withGraphs;
            this.bindings = bindings;
            this.varsSeen = varsSeen;
        }
    }
}
