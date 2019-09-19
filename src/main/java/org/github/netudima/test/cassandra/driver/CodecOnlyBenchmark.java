package org.github.netudima.test.cassandra.driver;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CodecOnlyBenchmark extends BenchmarkCommon {

    public static class SelectState extends DriverState {
        private PreparedStatement preparedSelect;

        @Override
        public void doSetupInternal(Session session) {
            preparedSelect = session.prepare("select clust_key, value1, value2, value3, value4, value5, value6, value7, value8" +
                    " from test_table_value8 where part_key=?");
        }
    }
    @State(Scope.Thread)
    public static class StatementState {
        private PreparedStatement preparedSelect;
        private Session session;

        List<Row> rows;

        @Setup
        public void setUp(SelectState state){
            this.preparedSelect = state.preparedSelect;
            this.session = state.session;
        }

        @Setup(Level.Iteration)
        public void doSetup() {
            BoundStatement statement = new BoundStatement(preparedSelect);
            statement.setString(0, "part_key1");
            ResultSet resultSet = session.execute(statement);
            rows = resultSet.all();
        }

    }

    @Benchmark
    public Object defaultCodecForString(StatementState state, Blackhole blackhole) {
        for (Row row : state.rows) {
            blackhole.consume(row.getString(0));
            blackhole.consume(row.getString(1));
            blackhole.consume(row.getString(2));
            blackhole.consume(row.getString(3));
            blackhole.consume(row.getString(4));
            blackhole.consume(row.getString(5));
            blackhole.consume(row.getString(6));
            blackhole.consume(row.getString(7));
        }
        return null;
    }

    @Benchmark
    public Object explicitCodecForString(StatementState state, Blackhole blackhole) {
        for (Row row : state.rows) {
            blackhole.consume(row.get(0, TypeCodec.varchar()));
            blackhole.consume(row.get(1, TypeCodec.varchar()));
            blackhole.consume(row.get(2, TypeCodec.varchar()));
            blackhole.consume(row.get(3, TypeCodec.varchar()));
            blackhole.consume(row.get(4, TypeCodec.varchar()));
            blackhole.consume(row.get(5, TypeCodec.varchar()));
            blackhole.consume(row.get(6, TypeCodec.varchar()));
            blackhole.consume(row.get(7, TypeCodec.varchar()));
        }
        return null;
    }
}
