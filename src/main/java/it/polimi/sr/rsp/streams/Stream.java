package it.polimi.sr.rsp.streams;

import com.espertech.esper.client.soda.CreateSchemaClause;
import com.espertech.esper.client.soda.SchemaColumnDesc;
import it.polimi.sr.rsp.utils.EncodingUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.jena.graph.Node_URI;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

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

    public String toEPLSchema() {
        CreateSchemaClause schema = new CreateSchemaClause();
        schema.setSchemaName(EncodingUtils.encode(iri.getURI()));
        schema.setInherits(new HashSet<String>(Arrays.asList(new String[]{"TStream"})));
        List<SchemaColumnDesc> columns = new ArrayList<SchemaColumnDesc>();
        schema.setColumns(columns);
        StringWriter writer = new StringWriter();
        schema.toEPL(writer);
        return writer.toString();
    }

    @Override
    public String toString() {
        return "Stream{" + "iri=" + iri + '}';
    }

}
