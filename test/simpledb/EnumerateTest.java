package simpledb;

import org.junit.Assert;
import org.junit.Test;
import simpledb.optimizer.JoinOptimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EnumerateTest {
    @Test
    public void testEquals() {
        int size = 20, setSize = 5;
        List<Integer> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(100 + i);
        }
        long start = System.currentTimeMillis();
        Set<Set<Integer>> e1 = JoinOptimizer.enumerateSubsetsOld(data, setSize);
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        Set<Set<Integer>> e2 = JoinOptimizer.enumerateSubsets(data, setSize);
        System.out.println(System.currentTimeMillis() - start);
        Assert.assertEquals(e1, e2);
    }
}
