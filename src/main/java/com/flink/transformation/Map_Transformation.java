package com.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Map_Transformation {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Long> inputDatatream = env.fromElements(1L,2L,3L,4L,5L);

		inputDatatream.map(new MapFunction<Long, Long>() {
		private static final long serialVersionUID = 1L;

		public Long map(Long value) throws Exception {
		    return value - 1 ;
		  }
		}).print();

		env.execute("Map Function Example");
	}

}
