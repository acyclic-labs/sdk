---------------------------- MODULE SwarmBudget ----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS UnsafeAllocation, UnsafeStep, UnsafeDepth

Agents == 1..6
Root == 1
MaxDepth == 2
SessionBudget == 4
StepBudget == 2

Parent(a) == IF a = 1 THEN 1 ELSE IF a = 2 THEN 1 ELSE
             IF a = 3 THEN 1 ELSE IF a = 4 THEN 2 ELSE
             IF a = 5 THEN 1 ELSE 4
Depth(a) == IF a = 1 THEN 0 ELSE IF a = 2 THEN 1 ELSE
            IF a = 3 THEN 1 ELSE IF a = 4 THEN 2 ELSE
            IF a = 5 THEN 2 ELSE 3

VARIABLES phase, allocated, totalAllocated, used

vars == <<phase, allocated, totalAllocated, used>>

Init == /\ phase = [a \in Agents |-> IF a = Root THEN "running" ELSE "absent"]
        /\ allocated = [a \in Agents |-> IF a = Root THEN 1 ELSE 0]
        /\ totalAllocated = 1
        /\ used = [a \in Agents |-> 0]

Fork(c) == /\ c \in Agents \ {Root}
           /\ phase[c] = "absent"
           /\ phase[Parent(c)] = "running"
           /\ (UnsafeDepth \/ Depth(c) <= MaxDepth)
           /\ (UnsafeAllocation \/ totalAllocated < SessionBudget)
           /\ totalAllocated < SessionBudget + 1
           /\ phase' = [phase EXCEPT ![c] = "admitted"]
           /\ allocated' = [allocated EXCEPT ![c] = 1]
           /\ totalAllocated' = totalAllocated + 1
           /\ UNCHANGED used

Start(c) == /\ c \in Agents
            /\ phase[c] = "admitted"
            /\ phase' = [phase EXCEPT ![c] = "running"]
            /\ UNCHANGED <<allocated, totalAllocated, used>>

Step(a) == /\ a \in Agents
           /\ phase[a] = "running"
           /\ used[a] < (IF UnsafeStep THEN StepBudget + 1 ELSE StepBudget)
           /\ used' = [used EXCEPT ![a] = @ + 1]
           /\ UNCHANGED <<phase, allocated, totalAllocated>>

Complete(a) == /\ a \in Agents
               /\ phase[a] = "running"
               /\ phase' = [phase EXCEPT ![a] = "completed"]
               /\ UNCHANGED <<allocated, totalAllocated, used>>

Next == (\E c \in Agents: Fork(c) \/ Start(c) \/ Complete(c))
        \/ (\E a \in Agents: Step(a))

Spec == Init /\ [][Next]_vars

TypeOK == /\ phase \in [Agents -> {"absent", "admitted", "running", "completed"}]
          /\ allocated \in [Agents -> 0..1]
          /\ totalAllocated \in 1..(SessionBudget + 1)
          /\ used \in [Agents -> 0..(StepBudget + 1)]

TotalBudgetConserved == totalAllocated <= SessionBudget
StepBudgetConserved == \A a \in Agents: used[a] <= StepBudget
DepthBounded == \A a \in Agents: phase[a] # "absent" => Depth(a) <= MaxDepth
=============================================================================
