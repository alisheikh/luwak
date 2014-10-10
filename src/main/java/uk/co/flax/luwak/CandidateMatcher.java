package uk.co.flax.luwak;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Class used to match candidate queries selected by a Presearcher from a Monitor
 * query index.
 */
public abstract class CandidateMatcher<T extends QueryMatch> implements Iterable<T> {

    private final List<MatchError> errors = new ArrayList<>();
    private final Map<String, T> matches = new HashMap<>();

    protected final InputDocument doc;

    private long queryBuildTime = -1;
    private long searchTime = System.nanoTime();
    private int queriesRun = -1;

    protected long slowLogLimit;

    protected final StringBuilder slowlog = new StringBuilder();

    /**
     * Creates a new CandidateMatcher for the supplied InputDocument
     * @param doc the document to run queries against
     */
    public CandidateMatcher(InputDocument doc) {
        this.doc = doc;
    }

    /**
     * Runs the supplied query against this CandidateMatcher's InputDocument, returning any
     * resulting match.
     *
     * @param queryId the query id
     * @param matchQuery the query to run
     * @param highlightQuery an optional query to use for highlighting.  May be null
     * @throws IOException
     * @return a QueryMatch object, or null if there was no match
     */
    public abstract T matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException;

    /**
     * Returns the QueryMatch for the given query, or null if it did not match
     * @param queryId the query id
     */
    public T matches(String queryId) {
        return matches.get(queryId);
    }

    protected void addMatch(String queryId, T match) {
        matches.put(queryId, match);
    }

    /**
     * @return the number of queries that matched
     */
    public int getMatchCount() {
        return matches.size();
    }

    @Override
    public Iterator<T> iterator() {
        return matches.values().iterator();
    }

    /**
     * Called by the Monitor if running a query throws an Exception
     * @param e the MatchError detailing the problem
     */
    public void reportError(MatchError e) {
        this.errors.add(e);
    }

    /**
     * @return a List of any MatchErrors created during the matcher run
     */
    public List<MatchError> getErrors() {
        return errors;
    }

    /**
     * @return the InputDocument for this CandidateMatcher
     */
    public InputDocument getDocument() {
        return doc;
    }

    /**
     * @return the id of the InputDocument for this CandidateMatcher
     */
    public String docId() {
        return doc.getId();
    }

    /**
     * @return how long (in ms) it took to build the Presearcher query for the matcher run
     */
    public long getQueryBuildTime() {
        return queryBuildTime;
    }

    /**
     * @return how long (in ms) it took to run the selected queries
     */
    public long getSearchTime() {
        return searchTime;
    }

    /**
     * @return the number of queries passed to this CandidateMatcher during the matcher run
     */
    public int getQueriesRun() {
        return queriesRun;
    }

    public void finish(long buildTime, int queryCount) {
        this.queryBuildTime = buildTime;
        this.queriesRun = queryCount;
        this.searchTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - searchTime, TimeUnit.NANOSECONDS);
    }

    /*
     * Called by the Monitor
     */
    public void setSlowLogLimit(long t) {
        this.slowLogLimit = t;
    }

    /**
     * Return the slow log for this match run.
     *
     * The slow log contains a list of all queries that took longer than the slow log
     * limit to run.
     *
     * @return the slow log
     */
    public String getSlowLog() {
        return slowlog.toString();
    }

}
