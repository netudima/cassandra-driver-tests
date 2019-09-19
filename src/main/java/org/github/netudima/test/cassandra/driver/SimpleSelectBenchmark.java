package org.github.netudima.test.cassandra.driver;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

public class SimpleSelectBenchmark extends BenchmarkCommon {

    public static class SelectState extends DriverState {
        private PreparedStatement preparedSelect;

        @Override
        public void doSetupInternal(Session session) {
            preparedSelect = session.prepare("select value from test_table where part_key=? and clust_key=?");
        }
    }

    @Benchmark
    public Object baseline(SelectState state) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString(0, "part_key1");
        statement.setString(1, "clust_key1");
        ResultSet resultSet = state.session.execute(statement);
        return resultSet.one();
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Dcom.datastax.driver.CHECK_IO_DEADLOCKS=false")
    public Object withDisabledDeadlockCheck(SelectState state) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString(0, "part_key1");
        statement.setString(1, "clust_key1");
        ResultSet resultSet = state.session.execute(statement);
        return resultSet.one();
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Dcom.datastax.driver.FLUSHER_SCHEDULE_PERIOD_NS=0")
    public Object withDisabledFlusherTimeSchedule(SelectState state) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString(0, "part_key1");
        statement.setString(1, "clust_key1");
        ResultSet resultSet = state.session.execute(statement);
        return resultSet.one();
    }
}
