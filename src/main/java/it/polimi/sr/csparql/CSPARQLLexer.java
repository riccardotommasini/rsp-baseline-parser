package it.polimi.sr.csparql;

import it.polimi.sr.sparql.SPARQL11Lexer;
import org.parboiled.Rule;

/**
 * Created by Riccardo on 12/08/16.
 */
public class CSPARQLLexer extends SPARQL11Lexer{

    public Rule REGISTER() {
        return StringIgnoreCaseWS("REGISTER");
    }

    public Rule QUERY() {
        return StringIgnoreCaseWS("QUERY");
    }

    public Rule EVERY() {
        return StringIgnoreCaseWS("EVERY");
    }


    public Rule COMPUTED() {
        return StringIgnoreCaseWS("COMPUTED");
    }

    public Rule TIME_UNIT() {
        return Sequence(FirstOf("ms", 's', 'm', 'h', 'd'), WS());
    }

    public Rule TRIPLES() {
        return StringIgnoreCaseWS("TRIPLES");
    }

    public Rule TUMBLING() {
        return StringIgnoreCaseWS("TUMBLING");
    }

    public Rule SLIDE() {
        return StringIgnoreCaseWS("SLIDE");
    }

    public Rule WINDOW() {
        return StringIgnoreCaseWS("WINDOW");
    }

    public Rule RANGE() {
        return StringIgnoreCaseWS("RANGE");
    }

    public Rule ON() {
        return StringIgnoreCaseWS("ON");
    }

    public Rule STREAM() {
        return StringIgnoreCaseWS("STREAM");
    }
}
