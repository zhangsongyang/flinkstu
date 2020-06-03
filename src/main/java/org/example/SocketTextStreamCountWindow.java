package org.example;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class SocketTextStreamCountWindow {
    public static void main(String[] args) throws Exception {
        //参数检查
//        if (args.length != 2) {
//            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
//            return;
//        }

        String hostname = "127.0.0.1";
        Integer port = 9000;


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStream<String> stream = env.socketTextStream(hostname, port);

        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = stream
                .flatMap(new LineSplitter())
                .keyBy(0)
                .countWindow(5)
                .sum(1);

        dataStream.print();

        env.execute("Java WordCount from SocketTextStream Example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                collector.collect(new Tuple2<String, Integer>("a", Integer.parseInt(token)));
            }
        }
    }
}
