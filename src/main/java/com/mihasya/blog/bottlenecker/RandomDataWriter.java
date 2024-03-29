package com.mihasya.blog.bottlenecker;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * index some random data into the configured Astyanax Pool
 */
public class RandomDataWriter implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RandomDataWriter.class);

    private final AstyanaxContext<Keyspace> keyspaceContext;
    private final String cfName;
    private final String prefix;
    private final int threads;
    private final int writesPerThread;
    private final int timeoutSec;

    private final AtomicLong totalWrites = new AtomicLong(0);

    public RandomDataWriter(Configuration config) {
        this.keyspaceContext = Util.getKeyspace(config);
        this.cfName = config.getString("bottlenecker.cf_name", "random_data");
        this.prefix = config.getString("bottlenecker.key_prefix", "");
        this.threads = config.getInt("bottlenecker.write_concurrency", 16);
        this.writesPerThread = config.getInt("bottlenecker.total_writes", Integer.MAX_VALUE) / threads;
        this.timeoutSec = config.getInt("bottlenecker.total_writes", 3);
    }

    @Override
    public void run() {
        final ColumnFamily<String, String> randomDataCf =
                new ColumnFamily<String, String>(
                        cfName,
                        StringSerializer.get(),
                        StringSerializer.get());

        ThreadGroup group = new ThreadGroup("indexer workers");
        for (int i = 0; i < threads; i++) {
            final int threadId = i;
            Thread t = new Thread(group, new Runnable() {
                @Override
                public void run() {
                    log.info("Thread {} starting up", threadId);
                    for (int i = 0; i < writesPerThread; i++) {
                        while (true) {
                            MutationBatch m = keyspaceContext.getEntity().prepareMutationBatch();
                            m.withRow(randomDataCf, String.format("%s%d_%d", prefix, threadId, i))
                                    .putColumn("name", UUID.randomUUID().toString());
                            try {
                                Future<OperationResult<Void>> resultFuture = m.executeAsync();
                                resultFuture.get(timeoutSec, TimeUnit.SECONDS);
                                if (i % 1000 == 0) {
                                    totalWrites.addAndGet(1000L);
                                }
                                break;
                            } catch (Exception e) {
                                log.error("Error indexing: ", e);
                            }
                        }

                    }
                }
            });
            t.setDaemon(false);
            t.start();
        }

        Thread reporter = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                    log.info("Written so far: {}", totalWrites.get());
                    } catch (Throwable e) {
                        log.error("Error in reporter thread: ", e);
                    }
                }
            }
        });
        reporter.start();
    }

    public static void main(String[] args) {
        // TODO do I need a proper config here?..
        new RandomDataWriter(new SystemConfiguration()).run();
    }
}
