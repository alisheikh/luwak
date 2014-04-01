package uk.co.flax.luwak;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.QueryTermExtractor;
import uk.co.flax.luwak.termextractor.RegexpNGramTermExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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
public class TestExtractors {

    @Test
    public void testRegexpExtractor() {

        RegexpNGramTermExtractor extractor = new RegexpNGramTermExtractor();
        List<QueryTerm> terms = new ArrayList<>();
        RegexpQuery query = new RegexpQuery(new Term("field", "super.*califragilistic"));

        extractor.extract(query, terms, null);

        assertThat(terms).containsExactly(new QueryTerm("field", "califragilistic", QueryTerm.Type.WILDCARD));

    }

    @Test
    public void testRangeQueriesReturnAnyToken() {
        QueryTermExtractor qte = new QueryTermExtractor();
        NumericRangeQuery<Long> nrq = NumericRangeQuery.newLongRange("field", 0l, 10l, true, true);
        Set<QueryTerm> terms = qte.extract(nrq);

        assertThat(terms).containsExactly(new QueryTerm("field", "", QueryTerm.Type.ANY));

        BooleanQuery bq = new BooleanQuery();
        bq.add(nrq, BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new Term("field", "term")), BooleanClause.Occur.MUST);

        terms = qte.extract(bq);
        assertThat(terms).containsExactly(new QueryTerm("field", "term", QueryTerm.Type.EXACT));
    }

}
