package backtype.storm.task;

import java.util.List;
import backtype.storm.tuple.Tuple;
import java.util.Collection;

public interface IOutputCollector {
    /**
        Returns the task ids that received the tuples.
    */
    List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple);
    void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple);
    void ack(Tuple input);
    void fail(Tuple input);
    void reportError(Throwable error);
}
