#!/bin/bash

system_tests=(AbortEvictionTest AggregateTest DeleteTest FilterTest InsertTest JoinTest QueryTest ScanTest TransactionTestAllDirty TransactionTestFive TransactionTestOne TransactionTestTen TransactionTestTwo)
for i in "${system_tests[@]}"; do
  cmd="ant runsystest -Dtest=${i}"
  echo ${i}
  eval ${cmd}
  if [ $? -ne 0 ]; then
    exit
  fi
done

tests=(AggregateTest BufferPoolWriteTest CatalogTest DeadlockTest FilterTest HeapFileReadTest HeapFileWriteTest HeapPageIdTest HeapPageReadTest HeapPageWriteTest InsertTest IntegerAggregatorTest IntHistogramTest JoinOptimizerTest JoinPredicateTest JoinTest LockingTest PredicateTest RecordIdTest StringAggregatorTest TableStatsTest TransactionTest TupleDescTest TupleTest)
for i in "${tests[@]}"; do
  cmd="ant runtest -Dtest=${i}"
  echo ${i}
  eval ${cmd}
  if [ $? -ne 0 ]; then
    exit
  fi
done