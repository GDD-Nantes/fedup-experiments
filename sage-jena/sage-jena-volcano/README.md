# Sage ⨯ Jena Volcano

Jena's engine relies on a paradigm called **Volcano** (also known as
iterator model). [Jena
ARQ](https://jena.apache.org/documentation/query/index.html) allows
practitioners to easily modify the chain of executions that represents
a query. This project creates the necessary factories to include
preemptive iterators directly inside this volcano model.

More specifically, it overrides Jena's default query engine over TDB
on basic core SPARQL operations:

- [X] **Basic graph patterns** and **quads** need to remember their location
  in the balanced tree storing the solutions.

- [X] **Filters** did not need any change unless they need an
  internal state.
  
- [ ] **Unions** need to remember the operation they stopped to.

- [ ] **Optionals** need to remember if they found a solution in the current partial solution.
