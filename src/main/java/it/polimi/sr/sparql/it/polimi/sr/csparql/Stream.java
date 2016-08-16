package it.polimi.sr.sparql.it.polimi.sr.csparql;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.jena.graph.Node_URI;

/**
 * Created by Riccardo on 14/08/16.
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class Stream {
    private Node_URI iri;
}
