package it.polimi.sr.mql.parser;

import it.polimi.sr.mql.MQLQuery;
import it.polimi.sr.mql.events.calculus.MatchClause;
import it.polimi.sr.mql.events.calculus.PatternCollector;
import it.polimi.sr.mql.events.declaration.EventDecl;
import it.polimi.sr.mql.events.declaration.IFDecl;
import it.polimi.sr.rsp.RSPQLParser;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.parboiled.Rule;

/**
 * Created by Riccardo on 09/08/16.
 */
public class MQLParser extends RSPQLParser {

    @Override
    public Rule Query() {
        return Sequence(push(new MQLQuery()), WS(), Optional(Registration()), Prologue(),
                ZeroOrMore(CreateEventClause()), EmitQuery(), EOI);
    }

    public Rule EmitQuery() {
        return Sequence(EmitClause(), ZeroOrMore(MatchClause()),
                DataClause(), WhereClause(), SolutionModifiers());
    }

    public Rule EmitClause() {
        return Sequence(EMIT(), pushQuery(((MQLQuery) popQuery(0)).setEmitQuery()),
                FirstOf(Sequence(ASTERISK(), pushQuery(((MQLQuery) popQuery(0)).setMQLQueryStar())),
                        OneOrMore(Sequence(Var(), pushQuery(((MQLQuery) pop(1)).addEmitVar((Node) pop()))))));
    }

    public Rule CreateEventClause() {
        return Sequence(CREATE(), EVENT(), Var(), OPEN_CURLY_BRACE(), EventDef(),
                push(new EventDecl((Var) pop(), match())), Optional(IfClause(), addIF((IFDecl) pop())),
                CLOSE_CURLY_BRACE(), pushQuery(((MQLQuery) popQuery(1)).addEventDecl((EventDecl) pop())));
    }

    public Rule IfClause() {
        return Sequence(IF(), OPEN_CURLY_BRACE(), TriplesBlock(), push(new IFDecl(popElement())), CLOSE_CURLY_BRACE());
    }

    public Rule EventDef() {
        return ZeroOrMore(Sequence(TestNot(FirstOf(IF(), CLOSE_CURLY_BRACE())), ANY), WS());
    }


    public Rule MatchClause() {
        return Sequence(MATCH(), PatternExpression(), setMatchClause());
    }

    public Rule PatternExpression() {
        return Sequence(FollowedByExpression(), Optional(Sequence(WITHIN(), LPAR(), TimeConstrain(),
                push(new PatternCollector(match(), (PatternCollector) pop())), RPAR())));
    }

    public Rule FollowedByExpression() {
        return Sequence(OrExpression(), ZeroOrMore(FirstOf(FOLLOWED_BY(), Sequence(NOT(), FOLLOWED_BY())),
                enclose(trimMatch()), OrExpression(), addExpression()));
    }

    public Rule OrExpression() {
        return Sequence(AndExpression(), ZeroOrMore(OR_(), enclose(trimMatch()), AndExpression(), addExpression()));
    }

    public Rule AndExpression() {
        return Sequence(QualifyExpression(),
                ZeroOrMore(AND_(), enclose(trimMatch()), QualifyExpression(), addExpression()));
    }

    public Rule QualifyExpression() {
        return FirstOf(Sequence(FirstOf(EVERY(), NOT()), push(new PatternCollector(trimMatch())), GuardPostFix(),
                addExpression()), GuardPostFix());
    }

    public Rule GuardPostFix() {
        return FirstOf(
                Sequence(LPAR(), PatternExpression(), RPAR(), push(new PatternCollector((PatternCollector) pop()))),
                Sequence(VarOrIRIref(), push(((MQLQuery) getQuery(-1)).getIfClause((Node) peek())),
                        push(new PatternCollector((IFDecl) pop(), (Node) pop()))));

    }


    //Utility methods

    @Override
    public boolean startSubQuery(int i) {
        return push(new MQLQuery(getQuery(i).getQ().getPrologue()));
    }

    public boolean setMatchClause() {
        ((MQLQuery) getQuery(-1)).addMatchClause(new MatchClause((PatternCollector) pop()));
        return true;
    }

    // MQL
    public boolean addIF(IFDecl pop) {
        pop.build();
        ((EventDecl) peek()).addIF(pop);
        return true;
    }


    public boolean addExpression() {
        PatternCollector inner = (PatternCollector) pop();
        PatternCollector outer = (PatternCollector) pop();
        outer.addPattern(inner);
        return push(outer);
    }

    public boolean enclose(String operator) {
        PatternCollector inner = (PatternCollector) pop();

        if (inner.isBracketed() || inner.getOperator() == null || !operator.equals(inner.getOperator())) {
            PatternCollector outer = new PatternCollector(operator);
            outer.setOperator(operator);
            outer.addPattern(inner);
            return push(outer);
        }
        return push(inner);

    }

    //MQL Syntax Extensions

    // MQL
    public Rule EVENT() {
        return StringIgnoreCaseWS("EVENT");
    }

    public Rule CREATE() {
        return StringIgnoreCaseWS("CREATE");
    }

    public Rule AND_() {
        return StringIgnoreCaseWS("AND");
    }

    public Rule OR_() {
        return StringIgnoreCaseWS("OR");
    }

    public Rule FOLLOWED_BY() {
        return FirstOf(StringWS("->"), StringIgnoreCaseWS("FOLLOWED_BY"),
                Sequence(StringIgnoreCaseWS("FOLLOWED"), BY()));
    }

    public Rule MATCH() {
        return StringIgnoreCaseWS("MATCH");
    }

    public Rule EMIT() {
        return StringIgnoreCaseWS("EMIT");
    }

    public Rule WITHIN() {
        return StringIgnoreCaseWS("WITHIN");
    }
}