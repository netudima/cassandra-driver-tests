
CREATE TABLE test_driver.test_table (
    part_key text,
    clust_key text,
    value text,
    PRIMARY KEY (part_key, clust_key)
);

insert into test_driver.test_table (part_key, clust_key, value) values ('part_key1', 'clust_key1', 'value');

CREATE TABLE test_driver.test_table_value8 (
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

ALTER TABLE test_driver.test_table_value8 WITH read_repair_chance = 0 AND dclocal_read_repair_chance = 0;
ALTER TABLE test_driver.test_table WITH read_repair_chance = 0 AND dclocal_read_repair_chance = 0;

insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key1', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key2', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key3', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key4', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key5', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key6', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key7', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');
insert into test_driver.test_table_value8 (part_key, clust_key, value1, value2, value3, value4, value5, value6, value7, value8) values (
'part_key1', 'clust_key8', 'value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8');