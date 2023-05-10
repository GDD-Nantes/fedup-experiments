# Sage ⨯ Jena benchmarks

Sage enhances Jena with pausing/resuming capabilities. This allows (i)
users to get complete results for their SPARQL queries; and (ii)
servers to fairly share the processing power among users.

However, this enhancement comes at a cost that we need to quantify. We
use [JMH](https://github.com/openjdk/jmh) to create benchmarks that
evaluate *Sage Query Engine* performance against the baseline of *TDB2
Query Engine*.

- [ ] Single triple pattern evaluation to measure the performance of
  the simplest building block: **scans**.
  
- [ ] Watdiv benchmark to evaluate the performance on **nested loop
  join** queries.
