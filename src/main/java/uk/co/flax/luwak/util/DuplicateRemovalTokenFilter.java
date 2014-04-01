package uk.co.flax.luwak.util;/*
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * A TokenFilter that removes tokens that have already been seen in the TokenStream
 */
public class DuplicateRemovalTokenFilter extends FilteringTokenFilter {

    private final CharArraySet seenTerms = new CharArraySet(Version.LUCENE_46, 1024, true);
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public DuplicateRemovalTokenFilter(TokenStream input) {
        super(Version.LUCENE_46, input);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        seenTerms.clear();
    }

    @Override
    protected boolean accept() throws IOException {
        return seenTerms.add(termAtt.subSequence(0, termAtt.length()));
    }

}
