package com.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class FilterFunction_Transformations {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Integer, Integer>> dataStream = env.fromElements(Tuple2.of(1, 2), Tuple2.of(3, 4),
				Tuple2.of(5, 6));
		/**
		 * Evaluates a boolean function for each element and retains those for
		 * which the function returns true. A filter that filters out null
		 * values
		 */
		dataStream.filter(new FilterFunction<Tuple2<Integer, Integer>>() {

			private static final long serialVersionUID = 1L;

			public boolean filter(Tuple2<Integer, Integer> arg0) throws Exception {
				return arg0 != null;
			}
		}).print();

	}

}
