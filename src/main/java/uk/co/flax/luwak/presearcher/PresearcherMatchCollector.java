package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.lucene.util.BytesRef;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.matchers.SimpleMatcher;

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
public class PresearcherMatchCollector
        extends Monitor.MatchingCollector<QueryMatch> implements IntervalCollector {

    private IntervalIterator positions;
    private StoredDocument document;
    private String currentId;

    private final PostingsHighlighter highlighter = new PostingsHighlighter(){
        @Override
        protected BreakIterator getBreakIterator(String field) {
            return BreakIterator.getWordInstance(Locale.ROOT);
        }
    };

    public final Map<String, StringBuilder> matchingTerms = new HashMap<>();

    public PresearcherMatchCollector(SimpleMatcher matcher) {
        super(matcher);
    }

    public Collection<PresearcherMatch> getMatches() {

    }

    @Override
    protected void doMatch(int doc, String queryId, BytesRef hash) throws IOException {

        mqDV.get(doc, serializedMQ);
        MonitorQuery mq = MonitorQuery.deserialize(serializedMQ);

        highlighter

        currentId = queryId;
        document = reader.document(doc);
        positions.scorerAdvanced(doc);
        while (positions.next() != null) {
            positions.collect(this);
        }

        super.doMatch(doc, queryId, hash);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        positions = scorer.intervals(true);
    }

    @Override
    public Weight.PostingFeatures postingFeatures() {
        return Weight.PostingFeatures.OFFSETS;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
        String terms = document.getField(interval.field).stringValue();
        if (!matchingTerms.containsKey(currentId))
            matchingTerms.put(currentId, new StringBuilder());
        matchingTerms.get(currentId)
                .append(" ")
                .append(interval.field)
                .append(":")
                .append(terms.substring(interval.offsetBegin, interval.offsetEnd));
    }

    @Override
    public void collectComposite(Scorer scorer, Interval interval, int docID) {

    }

}
