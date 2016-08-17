package it.polimi.sr.mql;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementPathBlock;

/**
 * Created by Riccardo on 16/08/16.
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
