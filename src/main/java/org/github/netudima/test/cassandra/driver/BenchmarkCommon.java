package org.github.netudima.test.cassandra.driver;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 60)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 60)
@Fork(2)
@Threads(value = 8)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkCommon {

    @State(Scope.Benchmark)
    public static class DriverState {
        public String contactPoint = System.getProperty("host", "localhost");
        public String username = System.getProperty("username");
        public String password = System.getProperty("password");

        public Cluster cluster;
        public Session session;

        @Setup(Level.Trial)
        public void doSetup() {
            cluster = Cluster.builder()
                    .addContactPoint(contactPoint)
                    .withAuthProvider(username != null ? new PlainTextAuthProvider(username, password) : AuthProvider.NONE)
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
                    .build();
            session = cluster.connect(System.getProperty("keyspace", "test_driver"));
            doSetupInternal(session);
        }

        public void doSetupInternal(Session session) {
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            doTearDownInternal(session);
            cluster.close();
        }

        public void doTearDownInternal(Session session) {
        }

    }
}
