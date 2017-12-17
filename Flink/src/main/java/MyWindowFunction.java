/*
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;

public class MyWindowFunction implements WindowFunction<Double, String, String, Window> {

  void apply(String key, Window window, Iterable<Tuple<String, Long>> input, Collector<Double> out) {
    long count = 0;
    for (Tuple<String, Long> in: input) {
      count++;
    }
    out.collect("Window: " + window + "count: " + count);
  }
}*/
