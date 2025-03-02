package io.phial.memory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collection;

public class ThreadCacheReference extends PhantomReference<ThreadCache> {
    private final Collection<Long> cache;

    public ThreadCacheReference(ThreadCache cache, ReferenceQueue<ThreadCache> q) {
        super(cache, q);
        this.cache = cache.getCache();
    }

    public Collection<Long> getCache() {
        return this.cache;
    }
}
