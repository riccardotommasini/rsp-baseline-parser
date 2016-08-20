package it.polimi.sr.mql.events.declaration;

import com.espertech.esper.client.soda.CreateSchemaClause;
import com.espertech.esper.client.soda.SchemaColumnDesc;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.jena.sparql.core.Var;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by Riccardo on 16/08/16. This class represents the event declaration
 * using DL manchester syntax. - The head consists of the left part of the DL
 * rule. - The body, //TODO parse it, is the right part of the DL rule. - ifdecl
 * is a SPARQL-Like constraint for the event instances.
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

    public String toEPLSchema() {
        CreateSchemaClause schema = new CreateSchemaClause();
        schema.setSchemaName(head);
        schema.setInherits(new HashSet<String>(Arrays.asList(new String[]{"TEvent"})));
        List<SchemaColumnDesc> columns = new ArrayList<SchemaColumnDesc>();
        for (Var var : ifdecl.getVars()) {
            SchemaColumnDesc scd = new SchemaColumnDesc();
            scd.setArray(false);
            scd.setType("String");
            scd.setName(var.getName());
            columns.add(scd);
        }
        schema.setColumns(columns);

        StringWriter writer = new StringWriter();
        schema.toEPL(writer);

        return writer.toString();
    }
}
