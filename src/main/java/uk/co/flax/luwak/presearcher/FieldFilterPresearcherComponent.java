package uk.co.flax.luwak.presearcher;

import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import uk.co.flax.luwak.InputDocument;

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

public class FieldFilterPresearcherComponent extends PresearcherComponent {

    private final String field;

    @Override
    public Query adjustPresearcherQuery(InputDocument doc, Query presearcherQuery) {

        String fieldValue = doc.getFieldValue(field);
        if (fieldValue == null)
            return presearcherQuery;

        BooleanQuery bq = new BooleanQuery();
        bq.add(presearcherQuery, BooleanClause.Occur.MUST);
        bq.add(buildFilterClause(fieldValue), BooleanClause.Occur.MUST);
        return bq;
    }

    private static final Splitter SPLITTER = Splitter.on(" ").omitEmptyStrings();

    protected Query buildFilterClause(String fieldValue) {
        List<String> values = Lists.newArrayList(SPLITTER.split(fieldValue));
        if (values.size() == 1)
            return new TermQuery(new Term(field, values.get(0)));

        BooleanQuery bq = new BooleanQuery();
        for (String value : values) {
            bq.add(new TermQuery(new Term(field, value)), BooleanClause.Occur.SHOULD);
        }
        return bq;
    }

    @Override
    public void adjustQueryDocument(Document doc, Map<String, String> metadata) {
        if (!metadata.containsKey(field))
            return;
        doc.add(new StringField(field, metadata.get(field), Field.Store.YES));
    }

    public FieldFilterPresearcherComponent(String field) {
        this.field = field;
    }

    @Override
    public TokenStream filterDocumentTokens(String field, TokenStream ts) {
        // We don't want tokens from this field to be present in the disjunction,
        // only in the extra filter query.  Otherwise, every doc that matches in
        // this field will be selected!
        if (this.field.equals(field))
            return new EmptyTokenStream();
        return ts;
    }
}
