package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;

import java.io.IOException;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    /**
     * Используем регулярное выражение для проверки входной строки
     */
    private final static Pattern regular = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},[0-7]$");

    /**
     * Регулярное выражения для разделения входной строки
     */
    private final static Pattern splitter = Pattern.compile(":|,");
    private Text word = new Text();

    /**
     * В методе map с помощью matcher проверяем с ошибкой ли строка,
     * если да, то записываем в ключ дату и час, а в значение номер сообщения из лога,
     * если нет, то увеличиваем счетчик битых строк
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (regular.matcher(line).matches()) {
            String[] parts = splitter.split(line);
            word.set(parts[0]);
            one.set(Integer.parseInt(parts[3]));
            context.write(word, one);
        }
        else {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
