package it.polimi.sr.rsp.streams;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.jena.graph.Node_URI;

/**
 * Created by Riccardo on 14/08/16.
 */
@AllArgsConstructor
public class Stream {
    @Override


    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Stream stream = (Stream) o;

        return iri != null ? iri.equals(stream.iri) : stream.iri == null;
    }

    @Override
    public int hashCode() {
        return iri != null ? iri.hashCode() : 0;
    }

    @Getter
    @Setter

    private Node_URI iri;
}
