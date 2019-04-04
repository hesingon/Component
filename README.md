# CS3223 Database implementation project

He Xing (A0143873Y)\
Kenneth Tan Xin You (A0135812L)\
Zhao Tianze (A0147989B)

## Project working/output path
\src\
\testcases

## Files modifications
\src\qp\QueryMain.java\
\src\qp\operators\JoinType.java\
All tables file: moved to testcases\
Added new experimental tables in testcases

### For block nested join:

\src\qp\operators\BLOCKNESTED.java

### For sort merge join:

\src\qp\operators\SortMergeJoin.java\
\src\qp\operators\ExternalMergeSort.java

### For distinct:

\src\qp\operators\Distinct.java

### For DP optimizer:

\src\qp\optimizer\DynamicProgrammingOptimizer.java\
\src\qp\optimizer\Optimizer.java\
\src\qp\optimizer\PlanCost.java
