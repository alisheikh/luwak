package uk.co.flax.luwak;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.AtomicReader;
import uk.co.flax.luwak.Monitor.CacheEntry;

/**
 * A Collector that decodes the stored query for each document hit.
 */
public abstract class MonitorQueryCollector extends TimedCollector {

    protected BinaryDocValues hashDV;
    protected BinaryDocValues idDV;
    protected AtomicReader reader;

    final BytesRef hash = new BytesRef();
    final BytesRef id = new BytesRef();

    protected Map<BytesRef, CacheEntry> queries;

    void setQueryMap(Map<BytesRef, CacheEntry> queries) {
        this.queries = queries;
    }

    /**
     * Finish collecting
     */
    protected abstract void finish();

    protected int queryCount = 0;
    protected long searchTime = -1;

    @Override
    public void setScorer(Scorer scorer) throws IOException {

    }

    @Override
    public final void setNextReader(AtomicReaderContext context) throws IOException {
        this.reader = context.reader();
        this.hashDV = context.reader().getBinaryDocValues(Monitor.FIELDS.hash);
        this.idDV = context.reader().getBinaryDocValues(Monitor.FIELDS.id);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    public int getQueryCount() {
        return queryCount;
    }

    public long getSearchTime() {
        return searchTime;
    }

    @Override
    public void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }
}
