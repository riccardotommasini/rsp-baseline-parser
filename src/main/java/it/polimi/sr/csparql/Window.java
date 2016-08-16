package it.polimi.sr.csparql;

import lombok.*;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;

import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Riccardo on 12/08/16.
 */
@Data
@NoArgsConstructor
@ToString(exclude = {"regex", "p"})
@EqualsAndHashCode
@RequiredArgsConstructor
public class Window {

    @NonNull
    private Node iri;
    private Integer beta;
    private Integer omega;
    private String unit_omega;
    private String unit_beta;
    private Stream stream;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    final private String regex = "([0-9]+)\\s*(ms|s|m|h|d|GRAPH|TRIPLES)";

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    final private Pattern p = Pattern.compile(regex);

    public Window addConstrain(String match) {
        //TODO hide visibility out of the package
        Matcher matcher = p.matcher(match);
        if (matcher.find()) {
            MatchResult res = matcher.toMatchResult();
            this.beta = this.omega = Integer.parseInt(res.group(1));
            this.unit_beta = this.unit_omega = res.group(2);
        }
        return this;
    }

    public Window addSlide(String match) {
        //TODO hide visibility out of the package
        Matcher matcher = p.matcher(match);
        if (matcher.find()) {
            MatchResult res = matcher.toMatchResult();
            this.beta = Integer.parseInt(res.group(1));
            this.unit_beta = res.group(2);
        }
        return this;
    }

    public Window addStreamUri(Node_URI uri) {
        if (stream == null) {
            stream = new Stream(uri);
        }
        stream.setIri(uri); //TODO hide visibility out of the package
        return this;
    }

}
