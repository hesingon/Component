/**
 * Base class for optimized plan exploration
 **/


package qp.optimizer;

import qp.utils.*;
import qp.operators.*;

import java.util.*;

public class DynamicProgrammingOptimizer extends Optimizer {

    /**
     * constructor
     *
     * @param sqlquery
     */
    public DynamicProgrammingOptimizer(SQLQuery sqlquery) {
        super(sqlquery);
    }

    @Override
    public Operator getOptimizedPlan() {

        // We make use of RandomInitialPlan for setting up the SCAN, SELECT, PROJECT operations
        // We are focusing on optimizing the join operations
        Plan plan = new Plan(sqlquery);
        Operator root = plan.prepareInitialPlan();
        return root;
    }

    class Plan extends RandomInitialPlan {

        private Map<BitSet, Integer> optimalCosts = new HashMap<>();
        private Map<BitSet, Operator> optimalPlans = new HashMap<>();
        private List<String> relations = new ArrayList<>();
        private Map<BitSet, Condition> joins = new HashMap<>();
        private Map<Integer, List<BitSet>> allPossibleBitSets = new HashMap<>();


        public Plan(SQLQuery sqlquery) {
            super(sqlquery);
        }

        private Optional<Condition> findConditionBetween(BitSet original, BitSet additional) {
            BitSet combined = orWithClone(original, additional);

            Optional<Condition> optionalCondition = Optional.empty();
            for (BitSet join : joins.keySet()) {

                if (isSupersetOf(combined, join) && !isSupersetOf(original, join)) {
                    if (optionalCondition.isPresent()) {
                        System.out.println("We are not prepared for two join conditions.");
                        System.exit(1);
                    }

                    Condition cn = joins.get(join);
                    String righttab = ((Attribute) cn.getRhs()).getTabName();

                    if (!righttab.equals(relations.get(additional.nextSetBit(0)))) {
                        cn.flip();
                    }

                    optionalCondition = Optional.of(cn);
                }
            }
            return optionalCondition;
        }

        private boolean conditionExists(BitSet bs) {
            for (BitSet join : joins.keySet()) {
                if (isSupersetOf(bs, join)) {
                    return true;
                }
            }
            return false;
        }

        private boolean isSupersetOf(BitSet bs1, BitSet bs2) {
            BitSet clone = (BitSet) bs1.clone();
            clone.and(bs2);
            return clone.equals(bs2);
        }

        private BitSet andNotWithClone(BitSet bs1, BitSet bs2) {
            BitSet clone = (BitSet) bs1.clone();
            clone.andNot(bs2);
            return clone;
        }

        private BitSet orWithClone(BitSet bs1, BitSet bs2) {
            BitSet clone = (BitSet) bs1.clone();
            clone.or(bs2);
            return clone;
        }

        @Override
        public void createJoinOp() {
            for (int i = 0; i < numJoin; i++) {
                // We only need to consider relations part of join plans.
                // There is no need to optimize the cartesian products
                // as we can just push them all the way to the end.

                Condition cn = (Condition) joinlist.elementAt(i);

                String lefttab = cn.getLhs().getTabName();
                String righttab = ((Attribute) cn.getRhs()).getTabName();
                if (!relations.contains(lefttab)) relations.add(lefttab);
                if (!relations.contains(righttab)) relations.add(righttab);
            }

            for (int i = 0; i < relations.size(); i++) {
                // get optimal plan for each relation
                // Here we assume that the plan generated for each relation in
                // RandomInitialPlan is optimal which needs to be verified.
                Operator op = (Operator) tab_op_hash.get(relations.get(i));
                BitSet bs = new BitSet();
                bs.set(i);

                optimalPlans.put(bs, op);
            }

            for (int i = 0; i < numJoin; i++) {
                Condition cn = (Condition) joinlist.elementAt(i);

                String lefttab = cn.getLhs().getTabName();
                String righttab = ((Attribute) cn.getRhs()).getTabName();

                BitSet join = new BitSet(relations.size());
                join.set(relations.indexOf(lefttab));
                join.set(relations.indexOf(righttab));

                joins.put(join, cn);
            }

            // Generate all possible bit set
            for (long i = 1; i <= Math.pow(2, relations.size()); i++) {
                BitSet bs = BitSet.valueOf(new long[]{i});
                int cardinality = bs.cardinality();

                if (!allPossibleBitSets.containsKey(cardinality)) {
                    allPossibleBitSets.put(cardinality, new ArrayList<>());
                }

                allPossibleBitSets.get(cardinality).add(bs);
            }

            assert allPossibleBitSets.get(relations.size()).size() == 1;
            for (int i = 2; i <= relations.size(); i++) {
                // for all subsets of length i
                List<BitSet> subsets = allPossibleBitSets.get(i);

                // get best plan for each subset
                for (int j = 0; j < subsets.size(); j++) {
                    BitSet currSubset = subsets.get(j);
                    Operator subOptimalPlan = getOptimalPlan(currSubset);

                    if (subOptimalPlan != null) {

                        System.out.println("------------------sub-optimal plan--------------");
                        Debug.PPrint(subOptimalPlan);
                        System.out.println(" " + optimalCosts.get(currSubset));
                    }

                }
            }

            BitSet withAllConditionSatisfied = allPossibleBitSets.get(relations.size()).get(0);
            root = getOptimalPlan(withAllConditionSatisfied);
            return;
        }

        private Operator getOptimalPlan(BitSet currSubset) {
            if (optimalPlans.containsKey(currSubset)) {
                return optimalPlans.get(currSubset);
            }

            int cardinality = currSubset.cardinality();
            assert cardinality > 1;

            int optimalCost = Integer.MAX_VALUE;
            Operator optimalPlan = null;

            if (conditionExists(currSubset)) {

                List<BitSet> prevSubsets = allPossibleBitSets.get(cardinality - 1);

                for (int k = 0; k < prevSubsets.size(); k++) {
                    BitSet prevSubset = prevSubsets.get(k);
                    if (!isSupersetOf(currSubset, prevSubset)) {
                        continue;
                    }

                    if (optimalPlans.get(prevSubset) == null) {
                        continue;
                    }

                    BitSet missing = andNotWithClone(currSubset, prevSubset);

                    Optional<Condition> optionalCondition = findConditionBetween(prevSubset,
                                                                                 missing);

                    if (!optionalCondition.isPresent()) {
                        continue;
                    }


                    Operator optPlanForPrevSubset = getOptimalPlan(prevSubset);
                    Operator optPlanForMissing = getOptimalPlan(missing);

                    Join jn = new Join(optPlanForPrevSubset,
                                       optPlanForMissing,
                                       optionalCondition.get(),
                                       OpType.JOIN);

                    Schema newsche = optPlanForPrevSubset.getSchema()
                                                         .joinWith(optPlanForMissing.getSchema());
                    jn.setSchema(newsche);

                    Operator bestJoinPlan = getBestJoinPlan(jn);
                    PlanCost pc = new PlanCost();
                    int cost = pc.getCost(bestJoinPlan);

                    if (cost < optimalCost) {
                        optimalCost = cost;
                        optimalPlan = bestJoinPlan;
                    }

                }
            }

            optimalCosts.put(currSubset, optimalCost);
            optimalPlans.put(currSubset, optimalPlan);
            return optimalPlan;
        }


        private Join getBestJoinPlan(Join node) {
            int lowestCost = Integer.MAX_VALUE;
            Join bestJoin = null;
            for (int i = 0; i < JoinType.numJoinTypes(); i++) {
                node.setJoinType(i);
                PlanCost pc = new PlanCost();
                int cost = pc.getCost(node);

                if (lowestCost > cost) {
                    bestJoin = node;
                }

                node = switchSubtree((Join) node.clone());

                pc = new PlanCost();
                cost = pc.getCost(node);

                if (lowestCost > cost) {
                    bestJoin = (Join) node.clone();
                }
            }
            return bestJoin;
        }
    }
}