package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import org.apache.jena.sparql.algebra.op.*;

/**
 * The visitor must implement this interface. Added value compared
 * to the default visitor {@link org.apache.jena.sparql.algebra.OpVisitor}:
 * it returns a type AND it passes arguments downstream.
 *
 * Remember to use the {@link ReturningArgsOpVisitorRouter} to call downstream visitors.
 * @param <R> The type of the object returned.
 * @param <A> The type of the argument to be passed.
 */
public class ReturningArgsOpVisitor<R, A> {
    public R visit(Mu mu, A args) {throw new UnsupportedOperationException("Mu");}
    public R visit(Mj mj, A args) {throw new UnsupportedOperationException("Mj");}
    public R visit(OpService req, A args) {throw new UnsupportedOperationException("Req");}

    public R visit(OpTriple triple, A args) {throw new UnsupportedOperationException("OpTriple");}
    public R visit(OpQuad quad, A args) {throw new UnsupportedOperationException("OpQuad");}
    public R visit(OpBGP bgp, A args) {throw new UnsupportedOperationException("OpBGP");}
    public R visit(OpSequence sequence, A args) {throw new UnsupportedOperationException("OpSequence");}
    public R visit(OpTable table, A args) {throw new UnsupportedOperationException("OpTable");}
    public R visit(OpLeftJoin lj, A args) {throw new UnsupportedOperationException("OpLeftJoin");}
    public R visit(OpConditional cond, A args) {throw new UnsupportedOperationException("OpConditional");}
    public R visit(OpFilter filter, A args) {throw new UnsupportedOperationException("OpFilter");}
    public R visit(OpUnion union, A args) {throw new UnsupportedOperationException("OpUnion");}
    public R visit(OpJoin join, A args) {throw new UnsupportedOperationException("OpJoin");}

    public R visit(OpDistinct distinct, A args) {throw new UnsupportedOperationException("OpDistinct");}
    public R visit(OpSlice slice, A args) {throw new UnsupportedOperationException("OpSlice");}
    public R visit(OpOrder orderBy, A args)  {throw new UnsupportedOperationException("OpOrder");}
    public R visit(OpProject project, A args) {throw new UnsupportedOperationException("OpProject");}
    public R visit(OpGroup groupBy, A args) {throw new UnsupportedOperationException("OpGroup");}
}
