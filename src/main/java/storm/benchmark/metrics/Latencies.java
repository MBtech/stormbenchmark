package storm.benchmark.metrics;

import org.apache.storm.metric.api.IMetric;
import java.util.ArrayList;
public class Latencies implements IMetric {
    ArrayList<Double> _latencies = new ArrayList<Double>();

    public Latencies() {
    }

    public void add(int latency) {
        _latencies.add((Double)1.0*latency);
    }

    public Object getValueAndReset() {
        ArrayList<Double> ret = _latencies;
        _latencies = new ArrayList<Double>();
        return ret;
    }
}
