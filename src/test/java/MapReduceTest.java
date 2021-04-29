import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, MapWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, MapWritable> mapReduceDriver;

    private final String testreducer = "2021-03-05 13";
    private final String testmapper = "2021-03-05 13:05:30,1";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    /* test mapper*/
    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testmapper))
                .withOutput(new Text(testreducer), new IntWritable(1))
                .runTest();
    }
    /*test reducer*/
    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(2));
        MapWritable result = new MapWritable();
        result.put(new Text("alert(1)"), new IntWritable(2));
        result.put(new Text("crit(2)"), new IntWritable(1));
        reduceDriver
                .withInput(new Text(testreducer), values)
                .withOutput(new Text(testreducer), result)
                .runTest();
    }
    /*test mapreduce*/
    @Test
    public void testMapReduce() throws IOException {
        MapWritable result = new MapWritable();
        result.put(new Text("alert(1)"), new IntWritable(1));
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testmapper))
                .withOutput(new Text(testreducer), result)
                .runTest();
    }
}
