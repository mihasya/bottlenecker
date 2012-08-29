package com.mihasya.blog.bottlenecker;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * read random data using a threadpool of configurable size
 */
public class RandomDataReader implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RandomDataReader.class);

    private final AstyanaxContext<Keyspace> keyspaceContext;

    // same config as write for part
    private final String cfName;
    private final String prefix;
    private final int writeThreads;
    private final int writesPerThread;
    private final int timeoutSec;

    // read specific config
    private final int readThreads;
    private final ConsistencyLevel readConsistencyLevel;

    RandomDataReader(Configuration config) {
        this.keyspaceContext = Util.getKeyspace(config);
        this.cfName = config.getString("bottlenecker.cf_name", "random_data");
        this.prefix = config.getString("bottlenecker.key_prefix", "");
        this.writeThreads = config.getInt("bottlenecker.write_concurrency", 16);

        // for the reader's use case, the total_writes value should be set to what we think the total number of writes
        // accomplished by the writer is
        this.writesPerThread = config.getInt("bottlenecker.total_writes", 3412000) / writeThreads;
        this.timeoutSec = config.getInt("bottlenecker.total_writes", 3);

        this.readThreads = config.getInt("bottlenecker.read_concurrency", 16);

        String readConsistencyLevelValue = config.getString("bottlenecker.read_cl", "CL_ONE");
        readConsistencyLevel = ConsistencyLevel.valueOf(readConsistencyLevelValue);
        Preconditions.checkNotNull(readConsistencyLevel);
    }

    private String getRandomKeyToRead() {
        int randomThread = (int)Math.min(writeThreads - 1, Math.round(Math.random() * writeThreads));
        int randomNumber = (int)Math.min(writesPerThread - 1, Math.round(Math.random() * writesPerThread));
        return String.format("%s%d_%d", prefix, randomThread, randomNumber);
    }

    @Override
    public void run() {
        final ColumnFamily<String, String> randomDataCf =
                new ColumnFamily<String, String>(
                        cfName,
                        StringSerializer.get(),
                        StringSerializer.get());
        ThreadGroup group = new ThreadGroup("reader workers");
        for (int i = 0; i < readThreads; i++) {
            final int threadId = i;
            Thread t = new Thread(group, new Runnable() {
                @Override
                public void run() {
                    log.info("Thread {} starting up!", threadId);
                    while (true) {
                        String key = getRandomKeyToRead();
                        ColumnFamilyQuery<String, String> query = keyspaceContext.getEntity().prepareQuery(randomDataCf);
                        query.setConsistencyLevel(readConsistencyLevel);
                        try {
                            Future<OperationResult<ColumnList<String>>> resultFuture = query.getKey(key).executeAsync();
                            OperationResult<ColumnList<String>> columns = resultFuture.get(timeoutSec, TimeUnit.SECONDS);
                            if (columns.getResult().getColumnNames().size() == 0) {
                                log.warn("MISS! empty read on key {}", key);
                            }
                        } catch (Exception e) {
                            log.error("Error reading: ", e);
                        }
                    }
                }
            });
            t.setDaemon(false);
            t.start();
        }
    }

    public static void main(String[] args) {
        new RandomDataReader(new SystemConfiguration()).run();
    }
}
