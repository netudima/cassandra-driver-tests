package org.github.netudima.test.cassandra.driver;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

/*
nodetool disableautocompaction

Used to check driver level optimizations

CREATE TABLE test.test_table_value8 (
    part_key text,
    clust_key text,
    value1 text,
    value2 text,
    value3 text,
    value4 text,
    value5 text,
    value6 text,
    value7 text,
    value8 text,
    PRIMARY KEY (part_key, clust_key)
);

insert into test.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key1', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');


 */
public class CodecSelectBenchmark extends BenchmarkCommon {

    public static class SelectState extends DriverState {
        private PreparedStatement preparedSelect;

        @Override
        public void doSetupInternal(Session session) {
            preparedSelect = session.prepare("select value1, value2, value3, value4, value5, value6, value7, value8" +
                    " from test.test_table_value8 where part_key=? and clust_key=?");
        }
    }

    @Benchmark
    public Object defaultCodecForString(SelectState state, Blackhole blackhole) {
        BoundStatement statement = new BoundStatement(state.preparedSelect);
        statement.setString(0, "part_key1");
        statement.setString(1, "clust_key1");
        ResultSet resultSet = state.session.execute(statement);
        Row row = resultSet.one();
        blackhole.consume(row.getString(0));
        blackhole.consume(row.getString(1));
        blackhole.consume(row.getString(2));
        blackhole.consume(row.getString(3));
        blackhole.consume(row.getString(4));
        blackhole.consume(row.getString(5));
        blackhole.consume(row.getString(6));
        blackhole.consume(row.getString(7));
        return row;
    }

    @Benchmark
       public Object explicitCodecForString(SelectState state, Blackhole blackhole) {
           BoundStatement statement = new BoundStatement(state.preparedSelect);
           statement.setString(0, "part_key1");
           statement.setString(1, "clust_key1");
           ResultSet resultSet = state.session.execute(statement);
           Row row = resultSet.one();
           blackhole.consume(row.get(0, TypeCodec.varchar()));
           blackhole.consume(row.get(1, TypeCodec.varchar()));
           blackhole.consume(row.get(2, TypeCodec.varchar()));
           blackhole.consume(row.get(3, TypeCodec.varchar()));
           blackhole.consume(row.get(4, TypeCodec.varchar()));
           blackhole.consume(row.get(5, TypeCodec.varchar()));
           blackhole.consume(row.get(6, TypeCodec.varchar()));
           blackhole.consume(row.get(7, TypeCodec.varchar()));
           return row;
       }
}
