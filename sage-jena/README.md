# Sage ⨯ Jena

[![Tests](https://github.com/Chat-Wane/sage-jena/actions/workflows/testing.yaml/badge.svg)](https://github.com/Chat-Wane/sage-jena/actions/workflows/testing.yaml)


[Jena](https://jena.apache.org/) is `A free and open source Java
framework for building Semantic Web and Linked Data applications.`
This incredible piece of work includes all components to build an
end-to-end software to evaluate SPARQL Query over RDF graphs. 
[Sage](http://sage.univ-nantes.fr/) is an approach that enables
pausing/resuming query execution as long as the data storage system
allows it.
This project provides the few additional components that enable **Sage
on top of Jena**. 

- [X] Jena provides a default data storage called **TDB2** that uses
  balanced plus trees that enables range queries. This project
  enhances it with the possibility to skip a range of values.

- [X] Jena's query engine follows the volcano (or iterator) model.
  This project provides a **Sage query engine** built on top of Jena's
  default one.

- [X] Jena's server is easily extensible through the use of modules.
  This project introduces Sage **Fuseki module** that intercepts the
  endpoint `query` operations to execute using enhanced operations,
  returning the results plus additional metadata that could be used to
  continue the execution later on.

- [ ] Jena's server also comes with an optional user interface.  This
  project provides a **Jena Fuseki UI** that slightly changes end
  users' UI so they can configure their query executions. For
  instance, whether or not they want an automatic re-execution of
  their query up till termination, whether or not they want random
  samples, etc.

- [ ] Sage provides additional features at the cost of additional
  computation. This project provides **benchmarks** that highlight the
  marginal cost of these great features.
