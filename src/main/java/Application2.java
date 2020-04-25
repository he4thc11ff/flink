import entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.Map;

public class Application2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Event> eventSource = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                String[] fields = s.split("[ ,.|]+");
                return new Event(Integer.valueOf(fields[0]), fields[1]);
            }
        });

        Pattern<Event, Event> start = Pattern.<Event>begin("begin").where(new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event event, IterativeCondition.Context<Event> context) throws Exception {
                return event.getId() >= 27;
            }
        });

        Pattern<Event, Event> end = start.next("final").where(new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event event, Context<Event> context) throws Exception {
                return event.getName().startsWith("l");
            }
        });

        PatternStream<Event> patternStream = CEP.pattern(eventSource, end);

        SingleOutputStreamOperator<String> selectedStream = patternStream.select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> map) throws Exception {
                StringBuilder sb = new StringBuilder();
                Event start1 = map.get("start").get(0);
                Event end1 = map.get("end").get(0);
                sb.append(start1.getId()).append("-").append(start1.getName()).append(" -> ")
                        .append(end1.getId()).append("-").append(end1.getName());
                return sb.toString();
            }
        });

        selectedStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, SinkFunction.Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("socket");
    }
}
