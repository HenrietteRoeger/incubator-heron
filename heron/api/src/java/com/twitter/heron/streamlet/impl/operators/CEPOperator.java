package com.twitter.heron.streamlet.impl.operators;

import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializablePredicate;

/**
 * An operator that detects a complex event in a window and emits it iff found.
 * else nothing.
 * 
 * @author roegerhe
 *
 */
public class CEPOperator<R, S, C> extends StreamletWindowOperator {
	private static final long serialVersionUID = -4748646871471052706L;
	private SerializableBiFunction<R, S, C> patternMatcher;
	S initialState;
	private OutputCollector collector;

	public CEPOperator(S initialState, SerializableBiFunction<R, S, C> patternMatcher) {
		this.initialState = initialState;
		this.patternMatcher = patternMatcher;
	}

	@Override
	public void prepare(Map<String, Object> heronConf, TopologyContext context, OutputCollector outputCollector) {
		collector = outputCollector;

	}

	@Override
	public void execute(TupleWindow inputWindow) {
		for (Tuple tuple : inputWindow.get()) {
			R tup = (R) tuple.getValue(0);
			C out = patternMatcher.apply(tup, initialState); // if the state changes, it needs to be handled in the
																// function! this gives the initial state only
			if (out != null) {
				collector.emit(new Values(out));
			}
		}
	}

}
