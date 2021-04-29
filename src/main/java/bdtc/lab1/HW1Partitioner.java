package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class HW1Partitioner extends Partitioner<Text, IntWritable> {
    /**
     *  Этот метод разделит все данные на 2 partitions, в соответствии с количеством редьюсеров
     *  в одну часть посылаем все события, которые происходили во время полуночи
     *  в другую часть посылаем все остальные данные
     * @param key This is value calculated by the mapper
     * @param value This is the value calculated by the reducer
     * @param numReduceTasks Specify the number of reducers
     */
    public int getPartition(Text key, IntWritable value, int numReduceTasks){
        if (numReduceTasks < 2)
            return 0;
        if (key.find("00") != -1)
            return 1 % numReduceTasks;
        return 0;
    }
}
