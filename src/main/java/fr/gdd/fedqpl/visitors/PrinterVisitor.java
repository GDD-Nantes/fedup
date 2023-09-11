package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;

/**
 * Write the plan in the standard output.
 */
public class PrinterVisitor extends FedQPLVisitor<Object, Integer> {

    public PrinterVisitor() {}

    public Object visit(FedQPLOperator op) {
        return op.visit(this, 0);
    }


    @Override
    public Object visit(Req req, Integer depth) {
        printNSpace(depth);
        System.out.println(String.format("req (%s) @ %s",
                req.getBgp().getPattern().toString(),
                req.getSource().toString()));
        return null;
    }

    @Override
    public Object visit(Mj mj, Integer depth) {
        printNSpace(depth);
        System.out.println("mj");
        mj.getChildren().forEach( op -> {
            op.visit(this, depth+1);
        });
        return null;
    }

    @Override
    public Object visit(Mu mu, Integer depth) {
        printNSpace(depth);
        System.out.println("mu");
        mu.getChildren().forEach( op -> {
            op.visit(this, depth+1);
        });
        return null;
    }

    @Override
    public Object visit(LeftJoin lj, Integer depth) {
        printNSpace(depth);
        System.out.println("leftjoin");
        lj.getLeft().visit(this, depth + 1);
        lj.getRight().visit(this, depth + 1);
        return null;
    }

    public void printNSpace(Integer spaces) {
        for (int i = 0; i < spaces; ++i) {
            System.out.print("\t");
        }
    }
}
