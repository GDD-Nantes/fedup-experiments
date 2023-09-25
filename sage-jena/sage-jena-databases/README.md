# Sage ⨯ Jena-databases

A utility sub-project that eases the creation of specific TDB2
databases for benchmarking (persistent) and testing (in
memory). Persistent database candidates are
[WatDiv](https://dsg.uwaterloo.ca/watdiv/) and
[WDBench](https://github.com/MillenniumDB/WDBench). However, they
weight respectively `1GB` and `162GB` which precludes online GitHub
actions. At the opposite, in memory databases are useful for
non-regression testing that can run with GitHub actions.

- [ ] Try to use [`act`](https://github.com/nektos/act) to test with
  persistent databases. This project allows developers to run GitHub 
  Actions locally, hence using their already set local databases. 
  However, this would require 2 sets of actions, one that run locally, and
  another online.
