package com.mihasya.blog.bottlenecker;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.BadHostDetectorImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.commons.configuration.Configuration;


public class Util {
    public static AstyanaxContext<Keyspace> getKeyspace(Configuration config) {
        String keyspace = config.getString("bottlenecker.keyspace", "bottlenecker");
        String seeds = config.getString("bottlenecker.pool", "localhost:9160");
        ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("Bottlenecker Pool");
        poolConfig.setSeeds(seeds);
        poolConfig.setBadHostDetector(new BadHostDetectorImpl(poolConfig));
        poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl(
                ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_UPDATE_INTERVAL,
                ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_RESET_INTERVAL,
                ConnectionPoolConfigurationImpl.DEFAULT_CONNECTION_LIMITER_WINDOW_SIZE,
                ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_BADNESS_THRESHOLD
        ));
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("bottlenecker cluster")
                .forKeyspace(keyspace)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(poolConfig)
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();

        return context;
    }
}
