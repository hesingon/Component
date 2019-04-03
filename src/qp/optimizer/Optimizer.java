package qp.optimizer;

import qp.operators.*;
import qp.utils.SQLQuery;

import java.util.Vector;

public abstract class Optimizer {

    SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy

    /**
     * constructor
     **/

    public Optimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }

    /**
     * implementation of Iterative Improvement Algorithm
     * * for Randomized optimization of Query Plan
     **/

    public abstract Operator getOptimizedPlan();

    /**
     * modifies the schema of operators which are modified due to selecing an alternative neighbor plan
     **/
    protected static void modifySchema(Operator node) {

        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            modifySchema(base);
            Vector attrlist = ((Project) node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        }
    }


    /**
     * Switches subtrees in-place
     * @param node
     * @return
     */
    protected static Join switchSubtree(Join node) {
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        /*** also flip the condition i.e.,  A X a1b1 B   = B X b1a1 A  **/
        node.getCondition().flip();

        /** modify the schema before returning the root **/
        modifySchema(node);
        return node;
    }
}