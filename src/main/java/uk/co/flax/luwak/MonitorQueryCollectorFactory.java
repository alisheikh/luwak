package uk.co.flax.luwak;

import java.util.Map;
import org.apache.lucene.util.BytesRef;

public interface MonitorQueryCollectorFactory {
    public MonitorQueryCollector get(Map<BytesRef, Monitor.CacheEntry> queryCache, CandidateMatcher matcher);
}
