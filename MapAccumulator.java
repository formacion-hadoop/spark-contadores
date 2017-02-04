package  com.formacionhadoop;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;



public class MapAccumulator implements AccumulatorParam<Map<String, Integer>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4101347584690424215L;

	@Override
	public Map<String, Integer> addAccumulator(Map<String, Integer> t1, Map<String, Integer> t2) {
		return mergeMap(t1, t2);
	}

	@Override
	public Map<String, Integer> addInPlace(Map<String, Integer> r1, Map<String, Integer> r2) {
		return mergeMap(r1, r2);

	}

	@Override
	public Map<String, Integer> zero(final Map<String, Integer> initialValue) {
		return new HashMap<>();
	}

	private Map<String, Integer> mergeMap(Map<String, Integer> map1, Map<String, Integer> map2) {

		Map<String, Integer> result = new HashMap<String, Integer>();
		result.putAll(map1);

		for (String key : map2.keySet()) {
			if (result.containsKey(key)) {
				Integer value = result.get(key) + map2.get(key);
				result.put(key, value);
			} else {
				result.put(key, map2.get(key));
			}
		}

		return result;
	}

}
