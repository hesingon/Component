/**
 * Base class for optimized plan exploration
 **/


package qp.optimizer;

import qp.utils.*;
import qp.operators.*;

import java.lang.Math;
import java.util.Vector;

public class Optimizer {

    SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy


    /**
     * constructor
     **/

    public Optimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }


    protected Operator getBestJoin(Operator left, Operator right, Condition cn) {
        int lowestCost = Integer.MAX_VALUE;
        Join bestJoin = null;
        PlanCost planCost = new PlanCost();
        for (int i = 0; i < JoinType.numJoinTypes(); i++) {
            Join node = new Join(left, right, cn, i);
            int cost = planCost.getCost(node);

            if (lowestCost > cost) {
                bestJoin = node;
            }

            node = switchSubtree(node);

            cost = planCost.getCost(node);

            if (lowestCost > cost) {
                bestJoin = node;
            }
        }
        return bestJoin;
    }

    protected Join switchSubtree(Join node) {
        System.out.println("------------------switch by commutative---------------");
        Join cloned = (Join) node.clone();
        Operator left = node.getLeft();
        Operator right = node.getRight();
        cloned.setLeft(right);
        cloned.setRight(left);
        /*** also flip the condition i.e.,  A X a1b1 B   = B X b1a1 A  **/
        cloned.getCondition().flip();

        /** modify the schema before returning the root **/
        modifySchema(node);
        return cloned;
    }

    /**
     * modifies the schema of operators which are modified due to selecting an alternative neighbor plan
     **/
    private void modifySchema(Operator node) {


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
}