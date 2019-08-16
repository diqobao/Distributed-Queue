package msgQ.common;

import java.io.Serializable;

public interface Record<T> extends Serializable {
    T getValue();
    String getTopic();
}
