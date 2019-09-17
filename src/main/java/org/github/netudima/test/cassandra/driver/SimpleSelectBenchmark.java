package org.github.netudima.test.cassandra.driver;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.openjdk.jmh.annotations.Benchmark;

/*

Used to check driver level optimizations

CREATE TABLE test.test_table (
    part_key text,
    clust_key text,
    value text,
    PRIMARY KEY (part_key, clust_key)
);

insert into test.test_table (part_key, clust_key, value) values ('part_key1', 'clust_key1', 'value');

 */
public class SimpleSelectBenchmark extends BenchmarkCommon {

    public static class SelectState extends DriverState {
        private PreparedStatement preparedSelect;

        @Override
        public void doSetupInternal(Session session) {
            preparedSelect = session.prepare("select value from test.test_table where part_key=? and clust_key=?");
        }
    }

    @Benchmark
    public Object test(SelectState state) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString(0, "part_key1");
        statement.setString(1, "clust_key1");
        ResultSet resultSet = state.session.execute(statement);
        return resultSet.one();
    }
}
