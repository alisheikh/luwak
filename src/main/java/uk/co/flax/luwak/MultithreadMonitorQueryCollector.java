package uk.co.flax.luwak;

import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class MultithreadMonitorQueryCollector extends Monitor.SearchingCollector {
    private final int PACKAGE_SIZE = 1000;

    private ExecutorService executorService;

    private final Map<BytesRef, Monitor.CacheEntry> queryCache;
    private final CandidateMatcher matcher;
    private final List<Monitor.CacheEntry> queries;
    private final List<Future<Object>> future = new LinkedList<>();

    private MultithreadMonitorQueryCollector(Map<BytesRef, Monitor.CacheEntry> queryCache, CandidateMatcher matcher) {
        this.queryCache = queryCache;
        this.matcher = matcher;
        this.queries = new LinkedList<>();
    }
    
    private class MatchTask implements Callable<Object> {

        private CandidateMatcher matcher;
        private List<Monitor.CacheEntry> queries;

        public MatchTask(CandidateMatcher matcher, List<Monitor.CacheEntry> queries) {
            this.matcher = matcher;
            this.queries = queries;
        }

        @Override
        public Object call() throws Exception {
            for (Monitor.CacheEntry query : queries) {
                try {
                    matcher.matchQuery(query.mq.getId(), query.matchQuery, query.highlightQuery);
                } catch (Exception ex) {
                    matcher.reportError(new MatchError(query.mq.getId(), ex));
                }
            }
            return null;
        }
    }

    @Override
    protected void doSearch(String queryId, BytesRef hash) {
        try {
            Monitor.CacheEntry entry = queryCache.get(hash);
            synchronized (queries) {
                queries.add(entry);
            }
        } catch (Exception e) {
            matcher.reportError(new MatchError(queryId, e));
        }
    }

    @Override
    protected void finish() {
        try {
            if (queries.isEmpty()) {
                return;
            }
            List<List<Monitor.CacheEntry>> queriesLists = Lists.partition(queries, PACKAGE_SIZE);
            List<MatchTask> tasks = new LinkedList<>();
            for (List<Monitor.CacheEntry> queriesList : queriesLists) {
                tasks.add(new MatchTask(matcher, queriesList));
            }
            executorService = Executors.newFixedThreadPool(queriesLists.size());
            future.addAll(executorService.invokeAll(tasks));
            for (Future<Object> task : future) {
                try {
                    task.get();
                } catch (ExecutionException | InterruptedException ex) {
                    matcher.reportError(new MatchError("", ex));
                }
            }
            executorService.shutdown();
        } catch (InterruptedException ex) {
            matcher.reportError(new MatchError("", ex));
        }
    }

    @Override
    public void setSearchTime(long searchTime) {
        matcher.setSearchTime(searchTime);
    }

    public static final MonitorQueryCollectorFactory FACTORY = new MonitorQueryCollectorFactory() {

        @Override
        public MonitorQueryCollector get(Map<BytesRef, Monitor.CacheEntry> queryCache, CandidateMatcher matcher) {
            return new MultithreadMonitorQueryCollector(queryCache, matcher);
        }

    };
}
