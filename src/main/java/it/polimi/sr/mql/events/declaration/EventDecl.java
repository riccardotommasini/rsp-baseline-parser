package it.polimi.sr.mql.events.declaration;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.jena.sparql.core.Var;

/**
 * Created by Riccardo on 16/08/16.
 This class represents the event declaration using DL manchester syntax.
 - The head consists of the left part of the DL rule.
 - The body, //TODO parse it, is the right part of the DL rule.
 - ifdecl is a SPARQL-Like constraint for the event instances.

 */
@Data
@EqualsAndHashCode
@ToString
public class EventDecl {

    private final String head;
    private final String body;
    private IFDecl ifdecl;

    public EventDecl(Var pop, String match) {
        this.head = pop.getVarName();
        this.body = match;
    }

    public EventDecl addIF(IFDecl pop) {
        pop.build();
        ifdecl = pop;
        return this;
    }
}
