package org.aksw.simba.quetsal.core;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.simba.quetsal.configuration.Summary;
import org.aksw.simba.quetsal.datastructues.HyperGraph.HyperEdge;
import org.aksw.simba.quetsal.datastructues.HyperGraph.Vertex;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.algebra.EmptyStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.cache.CacheEntry;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.optimizer.SourceSelection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.SubQuery;
import com.fluidops.fedx.util.QueryStringUtil;

/**
 * Perform triple pattern-wise source selection using Quetsal Sumaries, cache and SPARQL ASK. 
 * @author Saleem
 */
public class TBSSSourceSelection extends SourceSelection {
	static Logger log = LoggerFactory.getLogger(TBSSSourceSelection.class);
	
	public Map<HyperEdge, StatementPattern> hyperEdgeToStmt = new HashMap<HyperEdge,StatementPattern>(); //Hyper edges to Triple pattern Map
	public List<Map<String, Vertex>> theDNFHyperVertices = new ArrayList<Map<String, Vertex>>(); // Maps of vertices in different DNF hypergraphs

	final QuetzalConfig quetzalConfig;
		
	/**
	 * Constructor
	 * @param endpoints set of endpoints url
	 * @param cache cache
	 * @param query SPARQL query
	 */
	public TBSSSourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		super(endpoints, cache, queryInfo);
		quetzalConfig = queryInfo.getFederation().getConfig().getExtension();
	}
	
	public List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();

	private Vertex collectVertex(Var var, String label, Map<String, Vertex> v) {
		Vertex resultVertex = v.get(label);
		if (null == resultVertex) {
			resultVertex = new Vertex(var, label);
			v.put(label, resultVertex);
		}
		return resultVertex;
	}
	
	/**
	 * Perform triple pattern-wise source selection for the provided statement patterns
	 * of a SPARQL query using Quetzal Summaries, cache or remote ASK queries.
	 * Remote ASK queries are evaluated in parallel using the concurrency infrastructure
	 * of FedX. Note that this method is blocking until every source is resolved.
	 * Recent SPARQL ASK operations are cached for future use. The statement patterns are
	 * replaced by appropriate annotations in this process.
	 * Hypergraphs are created in step 1 of source selection and are used in step 2, i.e.
	 * prunning of triple pattern-wise selected sources. 
	 * @param bgpGroups BGPs
	 * @return
	 */
	@Override
	public void performSourceSelection(List<List<StatementPattern>> bgpGroups) {
		long start = System.currentTimeMillis();
		// Map statements to their sources. Use synchronized access!
		stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
		
		long tp = 0;
		for (List<StatementPattern> stmts : bgpGroups) {
			Map<String, Vertex> v = new HashMap<String, Vertex>();  
			for (StatementPattern stmt: stmts) { // for each statement determine the relevant sources.
				tp++;
				// cache.clear();
				stmtToSources.put(stmt, new ArrayList<StatementSource>());

				String s = null, p = null, o = null;
				Vertex sbjVertex, predVertex, objVertex;
				
				if (stmt.getSubjectVar().getValue() != null) {
					s = stmt.getSubjectVar().getValue().stringValue();
					sbjVertex = collectVertex(stmt.getSubjectVar(), s, v);
				} else {
					sbjVertex = collectVertex(stmt.getSubjectVar(), stmt.getSubjectVar().getName(), v);
				}
				
				if (stmt.getPredicateVar().getValue() != null) {
					p = stmt.getPredicateVar().getValue().stringValue();
					predVertex = collectVertex(stmt.getPredicateVar(), p, v);
				} else {
					predVertex = collectVertex(stmt.getPredicateVar(), stmt.getPredicateVar().getName(), v);
				}
				
				if (stmt.getObjectVar().getValue() != null) {
					o = stmt.getObjectVar().getValue().stringValue();
					objVertex = collectVertex(stmt.getObjectVar(), o, v);
				} else {
					objVertex = collectVertex(stmt.getObjectVar(), stmt.getObjectVar().getName(), v);
				}
				
				// labeling
				if (quetzalConfig.mode == QuetzalConfig.Mode.ASK_DOMINANT) {
					// System.out.println("ASK_DOMINANT");
					if (s == null && p == null && o == null) {
						for (Endpoint e : endpoints) 
							addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
					} else if (p != null) {
						if (p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && o != null) {
							lookupFedSumClass(stmt, p, o);
						} else if (!quetzalConfig.commonPredicates.contains(p) || (s == null && o == null)) {
							lookupFedSum(stmt, s /*sa*/, p, o /*oa*/);
						} else {
							cache_ASKselection(stmt);
						}
					} else {
						cache_ASKselection(stmt);
					}
				} else {
					// System.out.println("INDEX_DOMINANT");
					if (s == null && p == null && o == null) {
						// System.out.println("<<?s ?p ?o>> triple pattern... all members are relevant");
						for (Endpoint e : endpoints) {
							addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
						}
					} else if (s != null || p != null) {
						// System.out.println("s or p is not null");
						if ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p) && o != null) {
							// System.out.println("RDFType triple pattern");
							lookupFedSumClass(stmt, p, o);
						} else {
							// System.out.println("random triple pattern, need to check the index");
							lookupFedSum(stmt, s, p, o);
						}
					} else {
						cache_ASKselection(stmt);
					}
				}

				HyperEdge hEdge = new HyperEdge(sbjVertex, predVertex, objVertex);
				
				sbjVertex.outEdges.add(hEdge); predVertex.inEdges.add(hEdge); objVertex.inEdges.add(hEdge);
				hyperEdgeToStmt.put(hEdge, stmt);
			}
			theDNFHyperVertices.add(v);
		}
		long askStartTime = System.currentTimeMillis();
		log.info(String.format("ask time: %d, remote tasks: %d",  (askStartTime - start), remoteCheckTasks.size()));

		// sending ASK queries in parallel
		if (remoteCheckTasks.size() > 0) {
			SourceSelectionExecutorWithLatch.run(queryInfo.getFederation().getScheduler(), this, remoteCheckTasks, cache);
		}

		if (log.isDebugEnabled()) {
			log.debug("Number of ASK request: " + remoteCheckTasks.size());
		}

		// pruning
		int triplePatternWiseSources = 0 ;
		for (Map.Entry<StatementPattern, List<StatementSource>> stmtentry : stmtToSources.entrySet()) {
			// System.out.println("sources for " + stmtentry.getKey() + " = " + stmtentry.getValue());
			triplePatternWiseSources = triplePatternWiseSources + stmtentry.getValue().size();
		}
		if (triplePatternWiseSources > tp) {
			// System.out.println("pruning sources");
			stmtToSources = pruneSources(theDNFHyperVertices);
		}
		
		// adding sources to the query plan
		for (Map.Entry<StatementPattern, List<StatementSource>> stmtentry : stmtToSources.entrySet()) {
			StatementPattern stmt = stmtentry.getKey();
			List<StatementSource> sources = stmtentry.getValue();
			if (sources.size() > 1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
				for (StatementSource s : sources)
					stmtNode.addStatementSource(s);
				stmt.replaceWith(stmtNode);
			} else if (sources.size() == 1) {
				stmt.replaceWith(new ExclusiveStatement(stmt, sources.get(0), queryInfo));
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Statement " + QueryStringUtil.toString(stmt) + " does not produce any results at the provided sources, replacing node with EmptyStatementPattern.");
				}
				stmt.replaceWith( new EmptyStatementPattern(stmt));
			}
		}
	}

	/**
	 * Use cache and SPARQL ASK to perform relevant source selection for a statement pattern
	 * @param stmt statement pattern
	 */
	public void cache_ASKselection(StatementPattern stmt) {
		SubQuery subQuery = new SubQuery(stmt);
		for (Endpoint endpoint: endpoints) {
			StatementSourceAssurance assurance = cache.canProvideStatements(subQuery, endpoint);
			if (assurance == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
				addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.LOCAL));
			} else if (assurance == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
				addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));
			} else if (assurance == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {
				remoteCheckTasks.add(new CheckTaskPair(endpoint, stmt));
			}
		}
	}

	/**
	 * Get SPARQL query for index lookup
	 * @param sa Subject Authority
	 * @param p Predicate
	 * @param oa Object Authority
	 * @return queryString Query String
	 */
	public String getFedSumLookupQuery(String sa, String p, String oa) {
		String queryString = null;
		if (p != null) {
			if (sa == null && oa == null) {
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "	   ?s ds:url ?url ."
						+ "	   ?s ds:capability ?cap ."
						+ "	   ?cap ds:predicate <" + p + "> ."
						+ "}";
			} else if (sa != null && oa != null) {
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "    ?s ds:url ?url ."
						+ "    ?s ds:capability ?cap ."
						+ "	   ?cap ds:predicate <" + p + "> ."
						+ "    ?cap ds:sbjPrefix  ?sbjAuth ."
						+ "    ?cap ds:objPrefix  ?objAuth ."
						+ "    FILTER REGEX(STR(?sbjAuth), \"" +sa + "\", \"i\")"
						+ "    FILTER REGEX(STR(?objAuth), \"" +oa + "\", \"i\")"
						+ "}";	
			}
			else if (sa != null && oa == null) {
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "    ?s ds:url ?url ."
						+ "    ?s ds:capability ?cap . "
						+ "    ?cap ds:predicate <" + p + "> . "
						+ "    ?cap ds:sbjPrefix  ?sbjAuth ."
						+ "    FILTER REGEX(STR(?sbjAuth), \"" +sa + "\", \"i\")"
						+ "}";	
			}
			else if (sa == null && oa != null) {
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "    ?s ds:url ?url ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:predicate <" + p + "> ."
						+ "    ?cap ds:objPrefix  ?objAuth ."
						+ "    FILTER REGEX(STR(?objAuth), \"" +oa + "\", \"i\")"
						+ "}";	
			}
		} else {
			if (sa != null && oa != null) {
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "    ?s ds:url ?url ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:sbjPrefix  ?sbjAuth ."
						+ "    ?cap ds:objPrefix  ?objAuth ."
						+ "    FILTER REGEX(STR(?sbjAuth), \"" + sa + "\", \"i\")"
						+ "    FILTER REGEX(STR(?objAuth), \"" + oa + "\", \"i\")"
						+ "}";	
			} else if (sa != null && oa == null) {
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "    ?s ds:url ?url ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:sbjPrefix ?sbjAuth ."
						+ "    FILTER REGEX(STR(?sbjAuth), \"" +sa + "\", \"i\")"
						+ "}";	
			} else if (sa == null && oa != null) {
				queryString = "Prefix ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url WHERE {"
						+ "    ?s ds:url ?url ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:objPrefix  ?objAuth ."
						+ "    FILTER REGEX(STR(?objAuth), \"" +oa + "\", \"i\")"
						+ "}";	
			}
		}
		return queryString;
	}
	
	public void lookupFedSum(StatementPattern stmt, String s, String p, String o) {
		Set<String> ids = ((Summary)(queryInfo.getFedXConnection().getSummary())).lookupSources(stmt);
		// System.out.println("Sources for " + stmt + " = " + ids);
		if (ids != null && !ids.isEmpty()) {
			List<StatementSource> sources = stmtToSources.get(stmt);
			synchronized (sources) {
				for (String id : ids) {
					sources.add(new StatementSource(id, StatementSourceType.REMOTE));
				}
			}
		}
	}
	
	/**
	 * Quetzal Index lookup for rdf:type and its its corresponding values
	 * @param p Predicate i.e. rdf:type
	 * @param o Predicate value
	 * @param stmt Statement Pattern
	 */
	public void lookupFedSumClass(StatementPattern stmt, String p, String o) {
		Set<String> ids = ((Summary)(queryInfo.getFedXConnection().getSummary())).lookupSources(stmt);
		if (ids != null && !ids.isEmpty()) {
			List<StatementSource> sources = stmtToSources.get(stmt);
			synchronized (sources) {
				for (String id : ids) {
					sources.add(new StatementSource(id, StatementSourceType.REMOTE));
				}
			}
		}
	}
	
	static class PrefixSets {
		SortedSet<String> subjectSet = null;
		SortedSet<String> objectSet = null;
		SortedSet<String> predicateSet = null;
	}
	
	static class StatementPatternSourceDescriptor {
		Map<StatementSource, PrefixSets> srcToPrefixSets = null;
		SortedSet<String> subjectSetUnion = null;
		SortedSet<String> objectSetUnion = null;
		SortedSet<String> predicateSetUnion = null;
		
	}
	
	static interface SetProjection {
		SortedSet<String> getUnion(StatementPatternSourceDescriptor d);
		void setUnion(StatementPatternSourceDescriptor d, SortedSet<String> v);
		SortedSet<String> get(PrefixSets ps);
		void set(PrefixSets ps, SortedSet<String> v);
	}
	
	static class ObjectSetProjection implements SetProjection {
		@Override public SortedSet<String> getUnion(StatementPatternSourceDescriptor d) { return d.objectSetUnion; }
		@Override public void setUnion(StatementPatternSourceDescriptor d, SortedSet<String> s) { d.objectSetUnion = s; }
		@Override public SortedSet<String> get(PrefixSets ps) { return ps.objectSet; }
		@Override public void set(PrefixSets ps, SortedSet<String> v) { ps.objectSet = v; }
	}
	
	static class SubjectSetProjection implements SetProjection {
		@Override public SortedSet<String> getUnion(StatementPatternSourceDescriptor d) { return d.subjectSetUnion; }
		@Override public void setUnion(StatementPatternSourceDescriptor d, SortedSet<String> s) { d.subjectSetUnion = s; }
		@Override public SortedSet<String> get(PrefixSets ps) { return ps.subjectSet; }
		@Override public void set(PrefixSets ps, SortedSet<String> v) { ps.subjectSet = v; }
	}
	
	static class PredicateSetProjection implements SetProjection {
		@Override public SortedSet<String> getUnion(StatementPatternSourceDescriptor d) { return d.predicateSetUnion; }
		@Override public void setUnion(StatementPatternSourceDescriptor d, SortedSet<String> s) { d.predicateSetUnion = s; }
		@Override public SortedSet<String> get(PrefixSets ps) { return ps.predicateSet; }
		@Override public void set(PrefixSets ps, SortedSet<String> v) { ps.predicateSet = v; }
	}
	
	final static SubjectSetProjection subjectSetProjection = new SubjectSetProjection();
	final static PredicateSetProjection predicateSetProjection = new PredicateSetProjection();
	final static ObjectSetProjection objectSetProjection = new ObjectSetProjection();
	
	static class StringPair {
		String value0;
		String value1;
		
		public StringPair(String value0, String value1) {
			super();
			this.value0 = value0;
			this.value1 = value1;
		}
	}
	
	static SortedSet<String> handleEdge(StatementPatternSourceDescriptor d, SetProjection projection, Supplier<Collection<StatementSource>> srcsSuppl, Function<StatementSource, Set<String>> func) { // return union set of prefixes
		assert (projection.getUnion(d) == null);
		SortedSet<String> prefixUnionSet = new TreeSet<String>();
		if (d.srcToPrefixSets == null) {
			d.srcToPrefixSets = new HashMap<StatementSource, PrefixSets>();
			for (StatementSource src : srcsSuppl.get()) {
				Set<String> prefixes = func.apply(src);
				if (prefixes == null) {
					prefixes = func.apply(src);
					prefixes = new TreeSet<String>();
				}
				prefixUnionSet.addAll(prefixes); // union
				PrefixSets ps = new PrefixSets();
				projection.set(ps, new TreeSet<String>(prefixes));
				d.srcToPrefixSets.put(src, ps);
			}
		} else {
			for (Map.Entry<StatementSource, PrefixSets> srcentry : d.srcToPrefixSets.entrySet()) {
				Set<String> prefixes = func.apply(srcentry.getKey());
				prefixUnionSet.addAll(prefixes); // union
				projection.set(srcentry.getValue(), new TreeSet<String>(prefixes));
			}
		}
		projection.setUnion(d, prefixUnionSet);
		return prefixUnionSet;
	}
	
	StatementPatternSourceDescriptor handleStatement(StatementPattern stmt, SetProjection projection, Function<StatementSource, Set<String>> func, Map<StatementPattern, StatementPatternSourceDescriptor> /*out*/ stmtToPrefixes, SortedSet<String> /*out*/ prefixIntersectionSet) {
		StatementPatternSourceDescriptor d = stmtToPrefixes.get(stmt);
		if (d == null) {
			d = new StatementPatternSourceDescriptor();
			stmtToPrefixes.put(stmt, d);
		}
		SortedSet<String> prefixUnionSet = handleEdge(d, projection, () -> stmtToSources.get(stmt), func);
		clearUnion(prefixUnionSet);
		if (prefixIntersectionSet.isEmpty()) {
			prefixIntersectionSet.addAll(prefixUnionSet);
		} else {
			intersect(prefixIntersectionSet, prefixUnionSet);
		}
		return d;
	}
	
	void intersect(SortedSet<String> intersection, SortedSet<String> arg) {
		Collection<StringPair> replaces = new ArrayList<StringPair>();
		for (String s : intersection) {
			String sb = s + Character.MAX_VALUE;
			if (!arg.subSet(s, sb).isEmpty()) continue;
			SortedSet<String> head = arg.headSet(s);
			if (!head.isEmpty() && s.startsWith(head.last())) {
				replaces.add(new StringPair(s, head.last()));
			} else {
				replaces.add(new StringPair(s, null));
			}
		}
		for (StringPair p : replaces) {
			intersection.remove(p.value0);
			if (p.value1 != null) {
				intersection.add(p.value1);
			}
		}
	}
	
	void clearUnion(SortedSet<String> union) {
		Collection<String> dels = new ArrayList<String>();
		String prevV = "" + Character.MAX_VALUE;
		for (String v : union) {
			if (v.startsWith(prevV)) {
				dels.add(v);
			} else {
				prevV = v;
			}
		}
		for (String d : dels) {
			union.remove(d);
		}
	}
	
	static class StatementDescriptor {
		StatementPattern statement;
		StatementPatternSourceDescriptor spsd;
		SetProjection cut;
		
		public StatementDescriptor(StatementPattern statement, StatementPatternSourceDescriptor spsd, SetProjection cut) {
			super();
			this.statement = statement;
			this.spsd = spsd;
			this.cut = cut;
		}
	}
	
	/**
	 * Step 2 of Quetzal source selection. i.e. triple pattern-wise selected sources for hyperedge aka triple pattern
	 * @param dNFHyperVertices DNF groups (BGPs)of hypervertices
	 * @return Refine triple pattern-wise selected sources
	 */
	public Map<StatementPattern, List<StatementSource>> pruneSources(List<Map<String, Vertex>> dNFHyperVertices) {
		for (Map<String, Vertex> vs : dNFHyperVertices) {
			if (vs.size() > 3) { // only consider those DNF groups having at least 2 triple patterns
				Map<StatementPattern, StatementPatternSourceDescriptor> stmtToPrefixes = new HashMap<StatementPattern, StatementPatternSourceDescriptor>();
				for (Vertex v : vs.values()) {
					Collection<StatementDescriptor> sd = new ArrayList<StatementDescriptor>();
					SortedSet<String> prefixIntersectionSet = new TreeSet<String>();
					if (!v.inEdges.isEmpty() && !v.outEdges.isEmpty() && v.var.getValue() == null) { // hybrid node
						// System.out.println("pruning hybrid node " + v);
						for (HyperEdge inEdge : v.inEdges) {
							StatementPattern stmt = hyperEdgeToStmt.get(inEdge);
							// System.out.println("ingoing edge " + stmt);
							StatementPatternSourceDescriptor d = handleStatement(stmt, objectSetProjection, src -> {
								Set<String> objAuth = FedSumD_getMatchingObjAuthorities(stmt, src, v);
								// System.out.println("# obj auth for " + stmt + " on " + src + " = " + objAuth.size());
								return objAuth;
							}, stmtToPrefixes, prefixIntersectionSet);
							sd.add(new StatementDescriptor(stmt, d, objectSetProjection));
						}
						for (HyperEdge outEdge: v.outEdges) {
							StatementPattern stmt = hyperEdgeToStmt.get(outEdge);
							// System.out.println("outgoing edge " + stmt);
							StatementPatternSourceDescriptor d = handleStatement(stmt, subjectSetProjection, src -> {
								Set<String> subjAuth = getFedSumDMatchingSbjAuthorities(stmt, src);
								// System.out.println("# subj auth for " + stmt + " on " + src + " = " + subjAuth.size());
								return subjAuth;
							}, stmtToPrefixes, prefixIntersectionSet);
							sd.add(new StatementDescriptor(stmt, d, subjectSetProjection));
						}
						doSourcePrunning(sd, prefixIntersectionSet);
					} else if (v.outEdges.size() > 1) { // star node
						// System.out.println("pruning star node " + v);
						for (HyperEdge outEdge : v.outEdges) {
							StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
							// System.out.println("outgoing edge " + stmt);
							StatementPatternSourceDescriptor d = handleStatement(stmt, subjectSetProjection, src -> {
								Set<String> subjAuth = getFedSumDMatchingSbjAuthorities(stmt, src);
								// System.out.println("# subj auth for " + stmt + " on " + src + " = " + subjAuth.size());
								return subjAuth;
							}, stmtToPrefixes, prefixIntersectionSet);
							sd.add(new StatementDescriptor(stmt, d, subjectSetProjection));
						}
						doSourcePrunning(sd, prefixIntersectionSet);
					} else if (v.inEdges.size() > 1 && v.outEdges.isEmpty()) { // sink node
						// not implemented
					}
				}
			}
		}
		
		int newSources = 0;
		for (StatementPattern stmt: stmtToSources.keySet())
			newSources = newSources + stmtToSources.get(stmt).size();
		if (log.isDebugEnabled()) {
			log.debug("Total Triple pattern-wise sources selected : " + newSources);
		}

		return stmtToSources;
	}
	
	private void doSourcePrunning(Collection<StatementDescriptor> sds, SortedSet<String> prefixIntersectionSet) {
		Collection<StatementSource> sts2remove = new ArrayList<StatementSource>();
		for (StatementDescriptor sd : sds) {
			sts2remove.clear();
			for (Map.Entry<StatementSource, PrefixSets> srcentry : sd.spsd.srcToPrefixSets.entrySet()) {
				StatementSource src = srcentry.getKey();
				PrefixSets ps = srcentry.getValue();
				SortedSet<String> prefixes = sd.cut.get(ps);
				
				intersect(prefixes, prefixIntersectionSet); // updates in ps

				if (prefixes.isEmpty()) {
					sts2remove.add(src);
				}
			}
			for (StatementSource src: sts2remove) {
				sd.spsd.srcToPrefixSets.remove(src);
				List<StatementSource> sources = stmtToSources.get(sd.statement);
				sources.remove(src);
			}
		}
	}
	
	/**
	 *  Get matching Subject authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @return List of authorities
	 */
	public Set<String> getFedSumDMatchingSbjAuthorities(StatementPattern stmt, StatementSource src) {
		return ((Summary)(queryInfo.getFedXConnection().getSummary())).lookupSbjPrefixes(stmt, src.getEndpointID());
	}
	
	/**
	 *  A SPARQL query to retrieve matching subject authorities for a capable source of a triple pattern
	 * @param stmt Triple Pattern
	 * @param endPointUrl Url of the data source
	 * @return SPARQL query
	 */
	public String getFedSumSbjAuthLookupQuery(StatementPattern stmt,String endPointUrl) {
		String queryString = null;
		String s = null, p = null, o = null, sa = null, oa = null;
		if (stmt.getSubjectVar().getValue() != null) {
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa = sbjPrts[0]+"//"+sbjPrts[2];
		}  

		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		}

		if (stmt.getObjectVar().getValue() != null) {
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if (objPrts.length > 2) { // add only URI
				oa = objPrts[0] + "//" + objPrts[2];
			}
		}
		
		if (p != null) { // if predicate is bound
			queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
					+ "SELECT DISTINCT ?sbjAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:predicate <" + p + "> ."
					+ "    ?cap ds:sbjPrefix ?sbjAuth ."
					+ "}";	
		} else { // predicate is not bound
			if (sa == null && oa == null) { // and both subject and object are not bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?sbjAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + "> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:sbjPrefix ?sbjAuth ."
						+ "}";
			} else if (sa != null && oa != null) { // and both subject and object are bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?sbjAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + "> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:sbjPrefix  ?sbjAuth ."
						+ "    ?cap ds:objPrefix <" + oa + "> ."
						+ "    FILTER REGEX (str(?sbjAuth),'" + sa + "')"
						+ "}";
			} else if (sa != null && oa == null) { // and subject only is bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?sbjAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl+"> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:sbjPrefix ?sbjAuth ."
						+ "    FILTER REGEX (str(?sbjAuth),'" + sa + "')"
						+ "}";
			} else if (sa == null && oa != null) { // and object is bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?sbjAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + "> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:sbjPrefix  ?sbjAuth ."
						+ "    ?cap ds:objPrefix <" + oa + "> ."
						+ "}";
			}
		}
		return queryString;
	}
	
	/**
	 * Get matching object predicate authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @param v Vertex
	 * @return List of authorities
	 */
	public Set<String> FedSumD_getMatchingObjAuthorities(StatementPattern stmt, StatementSource src, Vertex v) {
		return ((Summary)(queryInfo.getFedXConnection().getSummary())).lookupObjPrefixes(stmt, src.getEndpointID());
	}
	
	/**
	 *  A SPARQL query to retrieve matching object authorities for a capable source of a triple pattern
	 * @param stmt Triple Pattern
	 * @param endPointUrl Url of the data source
	 * @param v Vertex
	 * @return SPARQL query
	 */
	public String getFedSumObjAuthLookupQuery(StatementPattern stmt, String endPointUrl, Vertex v) {
		String queryString = null;
		String s = null, p = null, o = null, sa = null, oa = null;
		if (stmt.getSubjectVar().getValue() != null) {
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa = sbjPrts[0] + "//" + sbjPrts[2];
		}

		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		} else if (stmt.getPredicateVar().getName().equals(v.label)) {
			p = stmt.getPredicateVar().getName();
		}

		if (stmt.getObjectVar().getValue() != null) {
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if (objPrts.length > 1) { // add only URI
				oa = objPrts[0] + "//" + objPrts[2];
			}
		}

		if(p != null && !stmt.getPredicateVar().getName().equals(v.label)) { // if predicate is bound
			queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
					+ "SELECT DISTINCT ?objAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:predicate <" + p + "> ."
					+ "    ?cap ds:objPrefix ?objAuth ."
				 	+ "}";	
		} else { // predicate is not bound
			if (sa == null && oa == null) { // and both subject and object are not bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?objAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + "> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:objPrefix ?objAuth ."
						+ "}";
			}
			else if (sa != null && oa != null) { // and both subject and object are bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?objAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + ">."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:objPrefix ?objAuth . "
						+ "    ?cap ds:objPrefix <" + oa + "> . "
						+ "    FILTER REGEX (str(?objAuth), '" + oa + "')"
						+ "}" ;
			} else if (sa == null && oa != null) { // and object only is bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?objAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + "> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:objPrefix ?objAuth ."
						+ "    FILTER REGEX (str(?objAuth), '" + oa + "')"
						+ "}" ;
			} else if (sa != null && oa == null) { // and subject is bound
				queryString = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?objAuth WHERE {"
						+ "    ?s ds:url <" + endPointUrl + "> ."
						+ "    ?s ds:capability ?cap ."
						+ "    ?cap ds:objPrefix ?objAuth ."
						+ "    ?cap ds:sbjPrefix <" + sa + "> ."
						+ "}" ;
			}
		}
		return queryString;
	}

	protected static class SourceSelectionExecutorWithLatch {

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch.
		 * Results are added to the map of the source selection instance. Errors 
		 * are reported as {@link OptimizationException} instances.
		 * @param hibiscusSourceSelection Quetsal Source Selection
		 * @param tasks Set of SPARQL ASK tasks
		 * @param cache Cache
		 */
		public static void run(ControlledWorkerScheduler scheduler, TBSSSourceSelection hibiscusSourceSelection, List<CheckTaskPair> tasks, Cache cache) {
			new SourceSelectionExecutorWithLatch(scheduler, hibiscusSourceSelection).executeRemoteSourceSelection(tasks, cache);
		}		

		private final TBSSSourceSelection sourceSelection;
		private final ControlledWorkerScheduler scheduler;

		private SourceSelectionExecutorWithLatch(ControlledWorkerScheduler scheduler, TBSSSourceSelection hibiscusSourceSelection) {
		    this.scheduler = scheduler;
			this.sourceSelection = hibiscusSourceSelection;
		}

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch
		 * 
		 * @param tasks
		 */
		private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
			if (tasks.isEmpty()) {
				return;
			}

			List<Exception> errors = new ArrayList<Exception>();
			List<Future<Void>> futures = new ArrayList<Future<Void>>();
			for (CheckTaskPair task : tasks) {
				futures.add(scheduler.schedule(new ParallelCheckTask(task.e, task.t, sourceSelection), QueryInfo.getPriority() + 1));
			}

			for (Future<Void> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					log.debug("Error during source selection. Thread got interrupted.");
					break;
				} catch (Exception e) {
					errors.add(e);
				}	
			}

			if (!errors.isEmpty()) {
				log.error(errors.size() + " errors were reported:");
				for (Exception e : errors) {
					log.error(ExceptionUtil.getExceptionString("Error occured", e));
				}
				Exception ex = errors.get(0);
				errors.clear();
				if (ex instanceof OptimizationException) {
					throw (OptimizationException) ex;
				}
				throw new OptimizationException(ex.getMessage(), ex);
			}
		}
	}


	public class CheckTaskPair {
		public final Endpoint e;
		public final StatementPattern t;
		public CheckTaskPair(Endpoint e, StatementPattern t){
			this.e = e;
			this.t = t;
		}		
	}


	/**
	 * Task for sending an ASK request to the endpoints (for source selection)
	 * 
	 * @author Andreas Schwarte
	 */
	protected static class ParallelCheckTask implements Callable<Void> {

		final Endpoint endpoint;
		final StatementPattern stmt;
		final TBSSSourceSelection sourceSelection;

		public ParallelCheckTask(Endpoint endpoint, StatementPattern stmt, TBSSSourceSelection sourceSelection) {
			this.endpoint = endpoint;
			this.stmt = stmt;
			this.sourceSelection = sourceSelection;
		}


		@Override
		public Void call() throws Exception {
			try {
				TripleSource t = endpoint.getTripleSource();
				RepositoryConnection conn = endpoint.getConn(); 

				boolean hasResults = t.hasStatements(stmt, conn, EmptyBindingSet.getInstance());

				CacheEntry entry = CacheUtils.createCacheEntry(endpoint, hasResults);
				sourceSelection.cache.updateEntry( new SubQuery(stmt), entry);

				if (hasResults) {
					sourceSelection.addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));
				}	

				return null;
			} catch (Exception e) {
				throw new OptimizationException("Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
			}
		}
	}
}