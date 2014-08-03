package com.github.javiercanillas.cassandra.driver;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.typesafe.config.*;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class purpose is to create a {@link com.datastax.driver.core.Cluster} from a
 * {@link com.typesafe.config.Config} structure
 * Created by JavierCanillas on 03/08/14.
 */
public class ClusterConfigurer {

    //a file containing

    /**
     * Static method to create a {@link com.datastax.driver.core.Cluster} from a file containing
     * {@link com.typesafe.config.Config} structure
     * @param filename full filepath with filename of the file containing configuration
     * @return a {@link com.datastax.driver.core.Cluster} instance
     */
    public static Cluster buildFromFile(String filename) {
        return buildFromConfig(ConfigFactory.load(filename));
    }

    /**
     * Static method to create a {@link com.datastax.driver.core.Cluster} from a
     * {@link com.typesafe.config.Config}
     * @param config an instance containing the necessary structure and values
     * @return a {@link com.datastax.driver.core.Cluster} instance
     */
    public static Cluster buildFromConfig(Config config) {
        Cluster.Builder builder = Cluster.builder();

        if (config.hasPath("port")) {
            builder = builder.withPort(config.getInt("port"));
        }

        if (config.hasPath("contactPoints")) {
            List<String> contactPoints = config.getStringList("contactPoints");
            builder = builder.addContactPoints(contactPoints.toArray(new String[contactPoints.size()]));
        } else {
            builder.addContactPointsWithPorts(buildInetSocketAddressList(config.getConfigList("contactPointsWithPorts")));
        }

        if (config.hasPath("compression")) {
            String compression = config.getString("compression");
            builder = builder.withCompression(ProtocolOptions.Compression.valueOf(compression));
        }

        if (config.hasPath("loadBalancingPolicy")) {
            LoadBalancingPolicy lbp = buildLoadBalancingPolicy(config.getConfig("loadBalancingPolicy"));
            builder = builder.withLoadBalancingPolicy(lbp);
        }

        if (config.hasPath("jmxReporting") && !config.getBoolean("jmxReporting")) {
            builder = builder.withoutJMXReporting();
        }

        if (config.hasPath("enableMetrics") && !config.getBoolean("enableMetrics")) {
            builder = builder.withoutMetrics();
        }

        if (config.hasPath("protocolVersion")) {
            builder = builder.withProtocolVersion(config.getInt("protocolVersion"));
        }

        if (config.hasPath("socketOptions")) {
            builder = builder.withSocketOptions(buildSocketOption(config.getConfig("socketOptions")));
        }

        if (config.hasPath("retryPolicy")) {
            builder = builder.withRetryPolicy(buildRetryPolicy(config.getConfig("retryPolicy")));
        }

        if (config.hasPath("reconnectionPolicy")) {
            builder = builder.withReconnectionPolicy(buildReconnectionPolicy(config.getConfig("reconnectionPolicy")));
        }
        if (config.hasPath("queryOptions")) {
            builder = builder.withQueryOptions(buildQueryOptions(config.getConfig("queryOptions")));
        }

        if (config.hasPath("poolingOptions")) {
            builder = builder.withPoolingOptions(buildPoolingOptions(config.getConfig("poolingOptions")));
        }

        return builder.build();
    }

    private static PoolingOptions buildPoolingOptions(Config config) {
        PoolingOptions po = new PoolingOptions();

        if (config.hasPath("coreConnectionPerHost")) {
            Config oneConfig = config.getConfig("coreConnectionPerHost");
            po.setCoreConnectionsPerHost(HostDistance.valueOf(oneConfig.getString("hostDistance")),
                    oneConfig.getInt("newCoreConnections"));
        }
        if (config.hasPath("maxConnectionPerHost")) {
            Config oneConfig = config.getConfig("maxConnectionPerHost");
            po.setMaxConnectionsPerHost(HostDistance.valueOf(oneConfig.getString("hostDistance")),
                    oneConfig.getInt("newCoreConnections"));
        }
        if (config.hasPath("maxSimultaneousRequestsPerConnectionThreshold")) {
            Config oneConfig = config.getConfig("maxSimultaneousRequestsPerConnectionThreshold");
            po.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.valueOf(oneConfig.getString("hostDistance")),
                    oneConfig.getInt("newCoreConnections"));
        }
        if (config.hasPath("minSimultaneousRequestsPerConnectionThreshold")) {
            Config oneConfig = config.getConfig("minSimultaneousRequestsPerConnectionThreshold");
            po.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.valueOf(oneConfig.getString("hostDistance")),
                    oneConfig.getInt("newCoreConnections"));
        }

        return po;
    }

    private static QueryOptions buildQueryOptions(Config config) {
        QueryOptions qo = new QueryOptions();
        if (config.hasPath("consistencyLevel")) {
            qo.setConsistencyLevel(ConsistencyLevel.valueOf(config.getString("consistencyLevel")));
        }
        if (config.hasPath("fetchSize")) {
            qo.setFetchSize(config.getInt("fetchSize"));
        }
        if (config.hasPath("serialConsistencyLevel")) {
            qo.setSerialConsistencyLevel(ConsistencyLevel.valueOf(config.getString("serialConsistencyLevel")));
        }
        return qo;
    }

    private static ReconnectionPolicy buildReconnectionPolicy(Config config) {
        ReconnectionPolicy rp = null;
        String name = config.getString("type");
        if (name.equalsIgnoreCase(ConstantReconnectionPolicy.class.getSimpleName())) {
            rp = buildConstantReconnectionPolicy(config.getConfig("configuration"));
        } else if (name.equalsIgnoreCase(ExponentialReconnectionPolicy.class.getSimpleName())) {
            rp = buildExponentialReconnectionPolicy(config.getConfig("configuration"));
        } else {
            throw new IllegalArgumentException("ReconnectionPolicy not supported: '" + name + "'");
        }
        return rp;
    }

    private static ReconnectionPolicy buildExponentialReconnectionPolicy(Config configuration) {
        return new ExponentialReconnectionPolicy(configuration.getInt("baseDelayMs"),
                configuration.getInt("maxDelayMs"));
    }

    private static ReconnectionPolicy buildConstantReconnectionPolicy(Config configuration) {
        return new ConstantReconnectionPolicy(configuration.getInt("constantDelayMs"));
    }

    private static RetryPolicy buildRetryPolicy(Config config) {
        RetryPolicy rp = null;
        String name = config.getString("type");
        if (name.equalsIgnoreCase(DefaultRetryPolicy.class.getSimpleName())) {
            rp = DefaultRetryPolicy.INSTANCE;
        } else if (name.equalsIgnoreCase(LoggingRetryPolicy.class.getSimpleName())) {
            rp = new LoggingRetryPolicy(buildRetryPolicy(config.getConfig("configuration")));
        } else if (name.equalsIgnoreCase(FallthroughRetryPolicy.class.getSimpleName())) {
            rp = FallthroughRetryPolicy.INSTANCE;
        } else if (name.equalsIgnoreCase(DowngradingConsistencyRetryPolicy.class.getSimpleName())) {
            rp = DowngradingConsistencyRetryPolicy.INSTANCE;
        } else {
            throw new IllegalArgumentException("RetryPolicy not supported: '" + name + "'");
        }
        return rp;
    }

    private static SocketOptions buildSocketOption(Config config) {
        SocketOptions so = new SocketOptions();
        if (config.hasPath("connectTimeoutMillis")) {
            so.setConnectTimeoutMillis(config.getInt("connectTimeoutMillis"));
        }
        if (config.hasPath("keepAlive")) {
            so.setKeepAlive(config.getBoolean("keepAlive"));
        }
        if (config.hasPath("readTimeoutMillis")) {
            so.setReadTimeoutMillis(config.getInt("readTimeoutMillis"));
        }
        if (config.hasPath("receiveBufferSize")) {
            so.setReceiveBufferSize(config.getInt("receiveBufferSize"));
        }
        if (config.hasPath("reuseAddress")) {
            so.setReuseAddress(config.getBoolean("reuseAddress"));
        }
        if (config.hasPath("sendBufferSize")) {
            so.setSendBufferSize(config.getInt("sendBufferSize"));
        }
        if (config.hasPath("soLinger")) {
            so.setSoLinger(config.getInt("soLinger"));
        }
        if (config.hasPath("tcpNoDelay")) {
            so.setTcpNoDelay(config.getBoolean("tcpNoDelay"));
        }
        return so;
    }

    private static LoadBalancingPolicy buildLoadBalancingPolicy(Config config) {
        LoadBalancingPolicy balancer = null;
        String name = config.getString("type");
        if (name.equalsIgnoreCase(TokenAwarePolicy.class.getSimpleName())) {
            balancer = buildLoadBalancingTokenAwarePolicy(config.getConfig("configuration"));
        } else if (name.equalsIgnoreCase(DCAwareRoundRobinPolicy.class.getSimpleName())) {
            balancer = buildLoadBalancingDCAwareRoundRobinPolicy(config.getConfig("configuration"));
        } else if (name.equalsIgnoreCase(LatencyAwarePolicy.class.getSimpleName())) {
            balancer = buildLoadBalancingLatencyAwarePolicy(config.getConfig("configuration"));
        } else if (name.equalsIgnoreCase(WhiteListPolicy.class.getSimpleName())) {
            balancer = buildLoadBalancingWhiteListPolicy(config.getConfig("configuration"));
        } else if (name.equalsIgnoreCase(RoundRobinPolicy.class.getSimpleName())) {
            balancer = buildLoadBalancingRoundRobinPolicy();
        } else {
            throw new IllegalArgumentException("Balancer not supported: '" + name + "'");
        }
        return balancer;
    }

    private static RoundRobinPolicy buildLoadBalancingRoundRobinPolicy() {
        return new RoundRobinPolicy();
    }

    private static WhiteListPolicy buildLoadBalancingWhiteListPolicy(Config configuration) {
        LoadBalancingPolicy childPolicy = buildLoadBalancingPolicy(configuration);
        List<InetSocketAddress> whiteList = buildInetSocketAddressList(configuration.getConfigList("whiteList"));
        WhiteListPolicy wlp = new WhiteListPolicy(childPolicy, whiteList);
        return null;
    }

    private static List<InetSocketAddress> buildInetSocketAddressList(List<? extends Config> whiteList) {
        List<InetSocketAddress> inetSocketAddresses = new LinkedList<InetSocketAddress>();
        Iterator<? extends Config> iterator = whiteList.iterator();
        while (iterator.hasNext()) {
            Config config = iterator.next();
            inetSocketAddresses.add(buildInetSocketAddress(config));
        }
        return inetSocketAddresses;
    }

    private static InetSocketAddress buildInetSocketAddress(Config config) {
        return new InetSocketAddress(config.getString("ip"), config.getInt("port"));
    }


    private static LatencyAwarePolicy buildLoadBalancingLatencyAwarePolicy(Config configuration) {
        LoadBalancingPolicy childPolicy = buildLoadBalancingPolicy(configuration);
        Double exclusionThreshold = getWithDefault(configuration, "exclusionThreshold", null);
        Long scale = getWithDefault(configuration, "scale", null);
        TimeUnit scaleTimeUnit = getWithDefault(configuration, "scaleTimeUnit", null);
        Long retryPeriod = getWithDefault(configuration, "retryPeriod", null);
        TimeUnit retryPeriodTimeUnit = getWithDefault(configuration, "retryPeriodTimeUnit", null);
        Long updateRate = getWithDefault(configuration, "updateRate", null);
        TimeUnit updateRateTimeUnit = getWithDefault(configuration, "updateRateTimeUnit", null);
        Integer minMeasure = getWithDefault(configuration, "minMeasure", null);

        LatencyAwarePolicy.Builder builder = LatencyAwarePolicy.builder(childPolicy);
        if (exclusionThreshold != null) {
            builder = builder.withExclusionThreshold(exclusionThreshold);
        }
        if (scale != null && scaleTimeUnit != null) {
            builder = builder.withScale(scale, scaleTimeUnit);
        }
        if (retryPeriod != null && retryPeriodTimeUnit != null) {
            builder = builder.withRetryPeriod(retryPeriod, retryPeriodTimeUnit);
        }
        if (updateRate != null && updateRateTimeUnit != null) {
            builder = builder.withUpdateRate(updateRate, updateRateTimeUnit);
        }
        if (minMeasure != null) {
            builder = builder.withMininumMeasurements(minMeasure);
        }

        return builder.build();
    }

    private static DCAwareRoundRobinPolicy buildLoadBalancingDCAwareRoundRobinPolicy(Config configuration) {
        String localDc = getWithDefault(configuration, "localDc", null);
        Integer usedHostsPerRemoteDc = getWithDefault(configuration, "usedHostsPerRemoteDc", null);
        Boolean allowRemoteDCsForLocalConsistencyLevel = getWithDefault(configuration, "allowRemoteDCsForLocalConsistencyLevel", null);

        DCAwareRoundRobinPolicy dcarrp = null;
        if (localDc != null && usedHostsPerRemoteDc != null && allowRemoteDCsForLocalConsistencyLevel != null) {
            dcarrp = new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc, allowRemoteDCsForLocalConsistencyLevel);
        } else if (localDc != null && usedHostsPerRemoteDc != null) {
            dcarrp = new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc);
        } else if (localDc != null) {
            dcarrp = new DCAwareRoundRobinPolicy(localDc);
        } else {
            dcarrp = new DCAwareRoundRobinPolicy();
        }
        return dcarrp;
    }

    private static TokenAwarePolicy buildLoadBalancingTokenAwarePolicy(Config configuration) {
        return new TokenAwarePolicy(buildLoadBalancingPolicy(configuration));
    }

    private static <T> T getWithDefault(Config config, String parameter, T defaultValue) {
        if (config.hasPath(parameter)) {
            return (T) config.getValue(parameter).unwrapped();
        } else {
            return defaultValue;
        }
    }
}
