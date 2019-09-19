package org.github.netudima.test.cassandra.driver;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;

public class AllOptimizationsBenchmark extends BenchmarkCommon {

    public static class SelectState extends DriverState {
        private PreparedStatement preparedSelect;

        @Override
        public void doSetupInternal(Session session) {
            preparedSelect = session.prepare("select clust_key, value1, value2, value3, value4, value5, value6, value7, value8" +
                    " from test_table_value8 where part_key=?");
        }
    }

    @Benchmark
    public Object baseline(SelectState state, Blackhole blackhole) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString("part_key", "part_key1");
        ResultSet resultSet = state.session.execute(statement);
        List<Row> rows = resultSet.all();
        for (Row row : rows) {
            blackhole.consume(row.getString("clust_key"));
            blackhole.consume(row.getString("value1"));
            blackhole.consume(row.getString("value2"));
            blackhole.consume(row.getString("value3"));
            blackhole.consume(row.getString("value4"));
            blackhole.consume(row.getString("value5"));
            blackhole.consume(row.getString("value6"));
            blackhole.consume(row.getString("value7"));
            blackhole.consume(row.getString("value8"));
        }
        return rows;
    }

    @Benchmark
    @Fork(jvmArgsAppend = {"-Dcom.datastax.driver.CHECK_IO_DEADLOCKS=false", "-Dcom.datastax.driver.FLUSHER_SCHEDULE_PERIOD_NS=0"})
    public Object allOptimizations(SelectState state, Blackhole blackhole) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString(0, "part_key1");
        ResultSet resultSet = state.session.execute(statement);
        List<Row> rows = resultSet.all();
        for (Row row : rows) {
            blackhole.consume(row.get(0, TypeCodec.varchar()));
            blackhole.consume(row.get(1, TypeCodec.varchar()));
            blackhole.consume(row.get(2, TypeCodec.varchar()));
            blackhole.consume(row.get(3, TypeCodec.varchar()));
            blackhole.consume(row.get(4, TypeCodec.varchar()));
            blackhole.consume(row.get(5, TypeCodec.varchar()));
            blackhole.consume(row.get(6, TypeCodec.varchar()));
            blackhole.consume(row.get(8, TypeCodec.varchar()));
        }
        return rows;
    }
}
