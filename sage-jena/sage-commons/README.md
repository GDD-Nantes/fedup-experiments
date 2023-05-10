# Sage-commons

Basic interface of Sage mostly featuring `Backend` and
`BackendIterator`. Implementing these interfaces eases the integration
of backends (such as [HDT](https://github.com/rdfhdt/hdt-java),
[TDB2](https://github.com/apache/jena/tree/main/jena-tdb2), or
[blazegraph](https://github.com/blazegraph/database)) with our code
generator targeting `Java`.  It also provides a comprehensive entry
point to pause/resume query executions.
