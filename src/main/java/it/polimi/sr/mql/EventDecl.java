package it.polimi.sr.mql;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.jena.sparql.core.Var;

/**
 * Created by Riccardo on 16/08/16.
 */
@Data
@EqualsAndHashCode
@ToString
public class EventDecl {
    private final String head;
    private final String body;

    public EventDecl(Var pop, String match) {
        this.head = pop.getVarName();
        this.body = match;
    }
}
