import bdtc.lab1.HW1Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PartitionerTest {
    private static final String firstKey = "2021-03-05 00";
    private static final String secondKey = "2021-03-05 01";

    private HW1Partitioner partitioner;

    @Before
    public void setUp() {
        partitioner = new HW1Partitioner();
    }

    /**
     * Задаем количество редьюсеров меньше двух
     */
    @Test
    public void lessThenTwoNumReduceTasks() {
        assertEquals(partitioner.getPartition(new Text(firstKey), new IntWritable(0), 0), 0);
        assertEquals(partitioner.getPartition(new Text(secondKey), new IntWritable(0), 1), 0);
    }

    /**
     * Задаем количество редьюсеров равное двум
     */
    @Test
    public void equalThreeNumReduceTasks() {
        assertEquals(partitioner.getPartition(new Text(firstKey), new IntWritable(0), 2), 1);
        assertEquals(partitioner.getPartition(new Text(secondKey), new IntWritable(0), 2), 0);
    }

}