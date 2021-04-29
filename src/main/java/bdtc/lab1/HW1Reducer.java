package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;

public class HW1Reducer extends Reducer<Text, IntWritable, Text, MapWritable> {

    /**
     * Используем переменную типа MapWritable, для того чтобы записать в нее количество разных сообщений из лога
     */
    private static Text keyWritable = new Text();
    private static MapWritable dictWritable = new MapWritable();
    private static final String[] values_key =
            {"emerg(0)", "alert(1)", "crit(2)", "error(3)", "warning(4)", "notice(5)", "info(6)", "debug(7)"};

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        keyWritable = new Text();
        dictWritable = new MapWritable();
        /**
         * К каждому сообщению из лога в dictWritable в значение (sum) добавляем единицу и так отдельно суммируем для каждого ключа
         */
        while (values.iterator().hasNext()) {
            int element = values.iterator().next().get();
            IntWritable sum = new IntWritable();
            keyWritable.set(new Text(values_key[element]));
            if (dictWritable.get(keyWritable) != null) {
                sum = (IntWritable) dictWritable.get(keyWritable);
                sum.set(sum.get() + 1);

                dictWritable.put(new Text(keyWritable), sum);
            }
            else {
                sum.set(1);
                dictWritable.put(new Text(keyWritable), sum);
            }
        }
        context.write(key, dictWritable);
    }
}
