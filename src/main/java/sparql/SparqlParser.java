package sparql;/*
 * Copyright (c) 2009 Ken Wenzel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.aggregate.AggregateRegistry;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.aggregate.AggregatorFactory;
import org.apache.jena.sparql.expr.aggregate.Args;
import org.apache.jena.sparql.syntax.*;
import org.apache.jena.sparql.util.ExprUtils;
import org.parboiled.BaseParser;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;

/**
 * SPARQL Parser
 *
 * @author Ken Wenzel, adapted by Mathias Doenitz
 */
@SuppressWarnings({"InfiniteRecursion"})
@BuildParseTree
public class SparqlParser extends BaseParser<Object> {
    // <Parser>

    public Query getQuery(String func, int i) {
        if (i == -1) {
            System.out.println("Unknown index from " + func);
            int size = getContext().getValueStack().size();
            i = size > 0 ? size - 1 : 0;
        }
        System.out.println("Query get, pos " + i);
        return (Query) peek(i);
    }

    public Query popQuery(int i) {

        if (i == -1) {
            System.out.println("Unknown index " + i);
            int size = getContext().getValueStack().size();
            i = size > 0 ? size - 1 : 0;

        }

        System.out.println("Query pop, pos " + i);
        return (Query) pop(i);
    }

    public boolean pushQuery(Query q) {
        return push(0, q);
    }


    public Element popElement() {
        return ((Element) pop());
    }

    public Rule Query() {
        return Sequence(push(new Query()), WS(), Prologue(), FirstOf(SelectQuery(), ConstructQuery())
                , EOI);   //, DescribeQuery(), AskQuery()
    }

    public Rule Prologue() {
        return Sequence(Optional(BaseDecl()), ZeroOrMore(PrefixDecl()));
    }

    public Rule BaseDecl() {
        return Sequence(BASE(), IRI_REF(), pushQuery(((Query) pop(1)).setBaseURI(match())));
    }

    public Rule PrefixDecl() {
        return Sequence(PrefixBuild(), pushQuery(((Query) pop(1)).setPrefix((Prefix) pop())));
    }

    public Rule PrefixBuild() {
        return Sequence(PREFIX(), PNAME_NS(), push(new Prefix(match())), IRI_REF(), push(((Prefix) pop()).setURI(match())));
    }

    public Rule SelectQuery() {
        return Sequence(
                SelectClause(),
                ZeroOrMore(DatasetClause()),
                WhereClause(),
                SolutionModifiers());
    }

    public Rule SelectClause() {
        return Sequence(SELECT(), pushQuery(popQuery(0).setSelectQuery()),
                Optional(
                        FirstOf(
                                Sequence(DISTINCT(), pushQuery(popQuery(0).setDistinct())),
                                Sequence(REDUCED(), pushQuery(popQuery(0).setReduced())))),
                FirstOf(Sequence(ASTERISK(), pushQuery(popQuery(0).setQueryStar())),
                        OneOrMore(
                                FirstOf(
                                        Sequence(Var(), pushQuery(((Query) pop(1)).addResultVar((Node) pop()))),
                                        Sequence(OPEN_BRACE(), Expression(), AS(), Var(), CLOSE_BRACE(),
                                                pushQuery(((Query) pop(2)).addResultVar((Node) pop(), (Expr) pop())))))


                ));
    }


    public Rule ConstructQuery() {
        return Sequence(
                FirstOf(
                        Sequence(
                                ConstructClause(),
                                ZeroOrMore(DatasetClause()),
                                WhereClause())
                        , Sequence(
                                ConstructWhereClause(), ZeroOrMore(DatasetClause())))
                , SolutionModifiers());
    }

    public Rule ConstructWhereClause() {
        return Sequence(CONSTRUCT(), pushQuery(popQuery(0).setConstructQuery()), WHERE(), OPEN_CURLY_BRACE(), pushQuery(popQuery(0).setConstructQuery()), push(new ElementGroup()), TriplesTemplate(), addTemplateToQuery(), addElementToQuery(), CLOSE_CURLY_BRACE());
    }

    public Rule ConstructClause() {
        return Sequence(CONSTRUCT(), pushQuery(popQuery(0).setConstructQuery()), ConstructTemplate(), addTemplateToQuery2());
    }


    public boolean addTemplateToQuery2() {
        getQuery("", 1).setConstructTemplate(new Template((((TripleCollectorBGP) pop()).getBGP())));
        return true;

    }

    public boolean addTemplateToQuery() {
        ((ElementGroup) peek(1)).addElement(new ElementPathBlock(((TripleCollectorBGP) peek()).getBGP()));
        getQuery("", 2).setConstructTemplate(new Template((((TripleCollectorBGP) pop()).getBGP())));
        return true;

    }

    public Rule DescribeQuery() {
        return Sequence(Sequence(DESCRIBE(), pushQuery(popQuery(0).setDescribeQuery())), FirstOf(OneOrMore(VarOrIRIref()),
                ASTERISK()), ZeroOrMore(DatasetClause()),
                Optional(WhereClause()), SolutionModifiers());
    }

    public Rule AskQuery() {
        return Sequence(Sequence(ASK(), pushQuery(popQuery(0).setAskQuery())), ZeroOrMore(DatasetClause()), WhereClause());
    }

    public Rule DatasetClause() {
        return Sequence(FROM(), FirstOf(DefaultGraphClause(),
                NamedGraphClause()));
    }

    public Rule DefaultGraphClause() {
        return Sequence(SourceSelector(), pushQuery(((Query) pop(1)).addGraphURI((Node_URI) pop())));
    }

    public Rule NamedGraphClause() {
        return Sequence(NAMED(), SourceSelector(), pushQuery(((Query) pop(1)).addNamedGraphURI((Node_URI) pop())));
    }

    public Rule SourceSelector() {
        return IriRef();
    }

    public Rule WhereClause() {
        return Sequence(WHERE(), GroupGraphPattern(), addElementToQuery());
    }

    public boolean addElementToQuery() {
        getQuery("", 1).addElement(popElement());
        return true;
    }

    public Rule SolutionModifiers() {
        return Sequence(Optional(GroupClause()), Optional(HavingClause()), Optional(OrderClause()), Optional(LimitOffsetClauses()));
    }

    public Rule GroupClause() {
        return Sequence(GROUP(), BY(), OneOrMore(GroupCondition()));
    }


    public Rule GroupCondition() {
        return FirstOf(Sequence(Var(), pushQuery(((Query) pop(1)).addGroupBy((Var) pop()))),
                Sequence(BuiltInCall(), pushQuery(((Query) pop(1)).addGroupBy((Expr) pop()))),
                Sequence(FunctionCall(), pushQuery(((Query) pop(1)).addGroupBy((Expr) pop()))),
                Sequence(OPEN_BRACE(), Expression(), AS(), Var(), CLOSE_BRACE(), pushQuery(((Query) pop(2)).addGroupBy((Var) pop(), (Expr) pop())))
        );
    }

    public Rule HavingClause() {
        return Sequence(HAVING(), OneOrMore(Constraint(), pushQuery(popQuery(1).addHavingCondition((Expr) pop()))));
    }

    public Rule HAVING() {
        return StringIgnoreCaseWS("HAVING");
    }


    public Rule OrderClause() {
        return Sequence(ORDER(), BY(), OneOrMore(OrderCondition()));
    }

    public Rule OrderCondition() {
        return FirstOf(
                Sequence(FirstOf(ASC(), DESC()), BrackettedExpression(), pushQuery(((Query) pop(2)).addOrderBy((Expr) pop(), pop().toString()))),
                Sequence(FirstOf(Constraint(), Var()), pushQuery(((Query) pop(1)).addOrderBy(pop()))));
    }

    public Rule LimitOffsetClauses() {
        return FirstOf(Sequence(LimitClause(), Optional(OffsetClause())),
                Sequence(OffsetClause(), Optional(LimitClause())));
    }

    public Rule LimitClause() {
        return Sequence(LIMIT(), INTEGER(), pushQuery(popQuery(0).setLimit(match())));
    }

    public Rule OffsetClause() {
        return Sequence(OFFSET(), INTEGER(), pushQuery(popQuery(0).setOffset(match())));
    }


    public Rule GroupGraphPattern() {
        debug("GroupGraphPattern");
        return Sequence(OPEN_CURLY_BRACE(), GroupGraphPatternSub(), CLOSE_CURLY_BRACE());
    }

    public Rule SubSelect() {
        return Sequence(OPEN_CURLY_BRACE(), push(new Query()), SelectClause(), WhereClause(), SolutionModifiers(), push(new ElementSubQuery(popQuery(0).getQ())), CLOSE_CURLY_BRACE());
    }

    public Rule GroupGraphPatternSub() {
        debug("GroupGraphPatternSub");
        return Sequence(push(new ElementGroup()), Optional(Sequence(TriplesBlock(), addSubElement())),
                ZeroOrMore(
                        Sequence(Sequence(GraphPatternNotTriples(), addSubElement()),
                                Optional(DOT()), Optional(Sequence(TriplesBlock(), addSubElement())))));
    }

    public boolean addSubElement() {
        debug("addSubElement");
        ((ElementGroup) peek(1)).addElement(popElement());
        return true;
    }


    public boolean addOptionalElement() {
        return push(new ElementOptional(popElement()));
    }

    public boolean createUnionElement() {
        return push(new ElementUnion(popElement()));
    }

    public boolean addUnionElement() {
        debug("addUnionElement");
        ((ElementUnion) peek(1)).addElement(popElement());
        return true;
    }

    public boolean addNamedGraphElement() {
        debug("addNamedGraphElement");
        return push(new ElementNamedGraph((Node) pop(), popElement()));
    }

    public Rule TriplesBlock() {
        return Sequence(TriplesSameSubject(), push(((TripleBuilder) pop()).buildPath()), Optional(Sequence(DOT(),
        Optional(Sequence(swap(), TriplesBlock(), addSubElement(), swap())))));
    }

    public Rule GraphPatternNotTriples() {
        return FirstOf(Filter(), OptionalGraphPattern(), GroupOrUnionGraphPattern(),
                GraphGraphPattern(), SubSelect());
    }

    public Rule OptionalGraphPattern() {
        debug("Optional");
        return Sequence(OPTIONAL(), GroupGraphPattern(),
                addOptionalElement());
    }


    public Rule GraphGraphPattern() {
        return Sequence(GRAPH(), VarOrIRIref(), GroupGraphPattern(), swap(), addNamedGraphElement());
    }

    public Rule GroupOrUnionGraphPattern() {
        return Sequence(GroupGraphPattern(), createUnionElement(), ZeroOrMore(Sequence(UNION(),
                GroupGraphPattern(), addUnionElement())));
    }

    public Rule Filter() {
        return Sequence(FILTER(), FilterConstraint(), addFilterElement());
    }

    public boolean addFilterElement() {
        return push(new ElementFilter((Expr) pop()));
    }

    public Rule FilterConstraint() {
        return FirstOf(BrackettedExpression(), BuiltInCallNoAggregates(), FunctionCall());
    }

    public Rule Constraint() {
        return FirstOf(BrackettedExpression(), BuiltInCall(), FunctionCall());
    }

    public Rule FunctionCall() {
        debug("FunctionCall");
        return Sequence(IriRef(), push(new Function(match())), ArgList(),
                push(((Function) pop(1)).add((Args) pop())),
                FirstOf(addFunctionCall(), addAggregateFunctionCall()));
    }

    public Rule addAggregateFunctionCall() {
        return Sequence(Test((AggregateRegistry.isRegistered(((Function) peek()).getIri()))),
                push(getQuery("addAggregateFunctionCall", 1).allocAggregate(((Function) pop()).createCustom())));
    }

    public boolean addFunctionCall() {
        return push(((Function) pop()).build());
    }

    public Rule ArgList() {
        return Sequence(push(new Args()), FirstOf(Sequence(OPEN_BRACE(), CLOSE_BRACE()), Sequence(
                OPEN_BRACE(), Expression(), addArg(), ZeroOrMore(Sequence(COMMA(),
                        Expression(), addArg())), CLOSE_BRACE())));
    }

    public boolean addArg() {
        ((Args) peek(1)).add((Expr) pop());
        return true;
    }


    public Rule TriplesTemplate() {
        return Sequence(TriplesSameSubject(), push(((TripleBuilder) pop()).buildTemplate()), Optional(Sequence(DOT(),
                Optional(Sequence(TriplesTemplate(), addTemplateToQuery())))));
    }


    public Rule ConstructTemplate() {

        return Sequence(OPEN_CURLY_BRACE(), ConstructTriples(),
                CLOSE_CURLY_BRACE());
    }

    public Rule ConstructTriples() {
        return Sequence(TriplesSameSubject(), push(((TripleBuilder) pop()).buildTemplate()), Optional(Sequence(DOT(),
                Optional(ConstructTriples()))));
    }

    public Rule TriplesSameSubject() {
        return FirstOf(Sequence(Sequence(VarOrTerm(), push(new TripleBuilder((Node) pop()))),
                PropertyListNotEmpty()),
                Sequence(TriplesNode(), PropertyList()));
    }

    public Rule PropertyListNotEmpty() {
        debug("PropertyListNotEmpty");
        return Sequence(
                Sequence(Verb(), push(((TripleBuilder) peek(1)).add((Node) pop())),
                        ObjectList(), drop())
                , ZeroOrMore(Sequence(SEMICOLON(),
                        Optional
                                (Sequence(Verb(), push(((TripleBuilder) peek(1)).add((Node) pop())), ObjectList(), drop())))));
    }

    public Rule PropertyList() {
        debug("PropertyList");
        return Optional(PropertyListNotEmpty());
    }

    public Rule ObjectList() {
        debug("ObjectList");
        return Sequence(Object_(), push(((TripleBuilder) peek(2)).add((Node) pop(), (Node) pop())),
                ZeroOrMore(Sequence(COMMA(), Object_(), push(((TripleBuilder) peek(2)).add((Node) pop(), (Node) pop())))));
    }

    public Rule Object_() {
        debug("Object_");
        return GraphNode();
    }

    public Rule Verb() {
        debug("Verb");
        return FirstOf(VarOrIRIref(), A());
    }

    public Rule TriplesNode() {
        debug("TriplesNode");
        return FirstOf(Collection(), BlankNodePropertyList());
    }

    public Rule BlankNodePropertyList() {
        return Sequence(OPEN_SQUARE_BRACE(), PropertyListNotEmpty(),
                CLOSE_SQUARE_BRACE());
    }

    public Rule Collection() {
        debug("Collection");
        return Sequence(OPEN_BRACE(), OneOrMore(GraphNode()), CLOSE_BRACE());
    }

    public Rule GraphNode() {
        debug("GraphNode");
        return FirstOf(VarOrTerm(), TriplesNode());
    }

    public Rule VarOrTerm() {
        return FirstOf(Var(), GraphTerm());
    }

    public Rule VarOrIRIref() {
        return FirstOf(Var(), IriRef());
    }

    public Rule Var() {
        return Sequence(FirstOf(VAR1(), VAR2()), allocVariable(trimMatch()));
    }

    public Rule GraphTerm() {
        return FirstOf(IriRef(), RdfLiteral(), NumericLiteral(),
                BooleanLiteral(), BlankNode(), Sequence(OPEN_BRACE(),
                        CLOSE_BRACE()));
    }

    public Rule ExpressionList() {

        return Sequence(OPEN_BRACE(), Expression(), push(new ExprList((Expr) pop())), ZeroOrMore(Sequence(COMMA(), Expression(), addExprToExprList())), CLOSE_BRACE());
    }

    public boolean addExprToExprList() {
        ((ExprList) peek(1)).add((Expr) pop());
        return true;
    }

    public Rule Expression() {
        return ConditionalOrExpression();
    }

    public Rule ConditionalOrExpression() {
        return Sequence(ConditionalAndExpression(), ZeroOrMore(Sequence(OR(),
                ConditionalAndExpression()), push(new E_LogicalOr((Expr) pop(), (Expr) pop()))));
    }

    public Rule ConditionalAndExpression() {
        return Sequence(ValueLogical(), ZeroOrMore(Sequence(AND(),
                ValueLogical(), push(new E_LogicalAnd((Expr) pop(), (Expr) pop())))));
    }

    public Rule ValueLogical() {
        return RelationalExpression();
    }

    public Rule RelationalExpression() {
        return Sequence(NumericExpression(), Optional(FirstOf(//
                Sequence(EQUAL(), NumericExpression(), swap(), push(new E_Equals((Expr) pop(), (Expr) pop()))), //
                Sequence(NOT_EQUAL(), NumericExpression(), swap(), push(new E_NotEquals((Expr) pop(), (Expr) pop()))), //
                Sequence(LESS(), NumericExpression(), swap(), push(new E_LessThan((Expr) pop(), (Expr) pop()))), //
                Sequence(GREATER(), NumericExpression(), swap(), push(new E_GreaterThan((Expr) pop(), (Expr) pop()))), //
                Sequence(LESS_EQUAL(), NumericExpression(), swap(), push(new E_LessThanOrEqual((Expr) pop(), (Expr) pop()))), //
                Sequence(GREATER_EQUAL(), NumericExpression(), swap(), push(new E_GreaterThanOrEqual((Expr) pop(), (Expr) pop()))) //
                ) //
        ));
    }

    public Rule NumericExpression() {
        return AdditiveExpression();
    }

    public Rule AdditiveExpression() {
        return Sequence(MultiplicativeExpression(), //
                ZeroOrMore(FirstOf(
                        Sequence(PLUS(), MultiplicativeExpression(),
                                push(new E_Add((Expr) pop(), (Expr) pop()))), //
                        Sequence(MINUS(), MultiplicativeExpression()//TODO DOUBLE_NEGATIVE
                                , swap(),
                                push(new E_Subtract((Expr) pop(), (Expr) pop()))))));
    }

    public Rule MultiplicativeExpression() {
        return Sequence(UnaryExpression(), ZeroOrMore(FirstOf(Sequence(
                ASTERISK(), UnaryExpression(),
                push(new E_Multiply((Expr) pop(), (Expr) pop()))), Sequence(DIVIDE(),
                UnaryExpression(), swap(), push(new E_Divide((Expr) pop(), (Expr) pop()))))));
    }

    public Rule UnaryExpression() {
        return FirstOf(Sequence(NOT(), PrimaryExpression()), Sequence(PLUS(),
                PrimaryExpression()), Sequence(MINUS(), PrimaryExpression()),
                PrimaryExpression());
    }

    public Rule PrimaryExpression() {
        return FirstOf(BrackettedExpression(), BuiltInCall(),
                IriRefOrFunction(), Sequence(RdfLiteral(), asExpr()), Sequence(NumericLiteral(), asExpr()),
                Sequence(BooleanLiteral(), asExpr()), Sequence(Var(), asExpr()));
    }

    public Rule BrackettedExpression() {
        return Sequence(OPEN_BRACE(), Expression(), CLOSE_BRACE());
    }

    public Rule BuiltInCall() {
        return FirstOf(
                Sequence(Aggregate(), push(getQuery("BuiltInCall", 1).allocAggregate((Aggregator) pop()))),
                BuiltInCallNoAggregates()
        );
    }

    public Rule BuiltInCallNoAggregates() {
        debug("BuiltInCall");
        return FirstOf(
                //TODO verify is the are all

                Sequence(STR(), OPEN_BRACE(), Expression(), push(new E_Str((Expr) pop())), CLOSE_BRACE()),
                Sequence(LANG(), OPEN_BRACE(), Expression(), push(new E_Lang((Expr) pop())), CLOSE_BRACE()),
                Sequence(LANGMATCHES(), OPEN_BRACE(), Expression(), COMMA(),
                        Expression(), push(new E_LangMatches((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(DATATYPE(), OPEN_BRACE(), Expression(), push(new E_Datatype((Expr) pop())), CLOSE_BRACE()),
                Sequence(BOUND(), OPEN_BRACE(), Var(), push(new E_Bound(new ExprVar((String) pop()))), CLOSE_BRACE()),
                Sequence(BNODE(), OPEN_BRACE(), Expression(), push(new E_BNode((Expr) pop())), CLOSE_BRACE()),
                Sequence(NIL(), push(new E_BNode())),
                Sequence(RAND(), push(new E_Random())),
                Sequence(AVG(), OPEN_BRACE(), Expression(), push(new E_NumAbs((Expr) pop())), CLOSE_BRACE()),
                Sequence(CEIL(), OPEN_BRACE(), Expression(), push(new E_NumCeiling((Expr) pop())), CLOSE_BRACE()),
                Sequence(FLOOR(), OPEN_BRACE(), Expression(), push(new E_NumFloor((Expr) pop())), CLOSE_BRACE()),
                Sequence(ROUND(), OPEN_BRACE(), Expression(), push(new E_NumRound((Expr) pop())), CLOSE_BRACE()),
                Sequence(CONCAT(), OPEN_BRACE(), ExpressionList(), push(new E_StrConcat((ExprList) pop())), CLOSE_BRACE()),

                Sequence(STRLEN(), OPEN_BRACE(), Expression(), push(new E_StrLength((Expr) pop())), CLOSE_BRACE()),
                Sequence(UCASE(), OPEN_BRACE(), Expression(), push(new E_StrUpperCase((Expr) pop())), CLOSE_BRACE()),
                Sequence(LCASE(), OPEN_BRACE(), Expression(), push(new E_StrUpperCase((Expr) pop())), CLOSE_BRACE()),
                Sequence(ENCODE_FOR_URI(), OPEN_BRACE(), Expression(), push(new E_StrEncodeForURI((Expr) pop())), CLOSE_BRACE()),
                Sequence(CONTAINS(), OPEN_BRACE(), Expression(), COMMA(), Expression(), swap(), push(new E_StrContains((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(SAME_TERM(), OPEN_BRACE(), Expression(), COMMA(), Expression(), swap(), push(new E_SameTerm((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(STRDT(), OPEN_BRACE(), Expression(), COMMA(), Expression(), swap(), push(new E_StrDatatype((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(STRLANG(), OPEN_BRACE(), Expression(), COMMA(), Expression(), swap(), push(new E_StrLang((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(IF(), OPEN_BRACE(), Expression(), COMMA(), Expression(), COMMA(), Expression(), swap3(), push(new E_Conditional((Expr) pop(), (Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(SUBSTR(), OPEN_BRACE(), Expression(), COMMA(), Expression(), COMMA(), Expression(), swap3(), push(new E_StrSubstring((Expr) pop(), (Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(REPLACE(), OPEN_BRACE(), Expression(), COMMA(), Expression(), COMMA(), Expression(), COMMA(), Expression(), swap4(), push(new E_StrReplace((Expr) pop(), (Expr) pop(), (Expr) pop(), (Expr) pop())), CLOSE_BRACE()), //TODO check swap4

                Sequence(ISIRI(), OPEN_BRACE(), Expression(), push(new E_IsIRI((Expr) pop())), CLOSE_BRACE()),
                Sequence(ISURI(), OPEN_BRACE(), Expression(), push(new E_IsURI((Expr) pop())), CLOSE_BRACE()),
                Sequence(ISBLANK(), OPEN_BRACE(), Expression(), push(new E_IsBlank((Expr) pop())), CLOSE_BRACE()),
                Sequence(ISLITERAL(), OPEN_BRACE(), Expression(), push(new E_IsLiteral((Expr) pop())), CLOSE_BRACE()),
                Sequence(IS_NUMERIC(), OPEN_BRACE(), Expression(), push(new E_IsNumeric((Expr) pop())), CLOSE_BRACE()),

                Sequence(SAMETERM(), OPEN_BRACE(), Expression(), COMMA(),
                        Expression(), push(new E_SameTerm((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),

                RegexExpression());
    }

    public Rule Aggregate() {
        return FirstOf(
                Count(),
                Sum(),
                Min(),
                Max(),
                Avg(),
                Sample(),
                GroupContant());

    }

    public Rule GroupContant() {
        return Sequence(GROUP_CONCAT(),
                OPEN_BRACE(),
                FirstOf(
                        Sequence(DISTINCT(), push(new Boolean(true))),
                        push(new Boolean(false))),
                Sequence(
                        Expression(),
                        SEMICOLON(),
                        SEPARATOR(),
                        EQUAL(),
                        String(),
                        swap(),
                        push(AggregatorFactory.createGroupConcat((Boolean) pop(),
                                (Expr) pop(), trimMatch(), new ExprList()))
                ), CLOSE_BRACE());
    }

    public Rule SEPARATOR() {
        return StringIgnoreCaseWS("SEPARATOR");
    }

    public Rule Sample() {
        return Sequence(SAMPLE(), OPEN_BRACE(), FirstOf(
                Sequence(DISTINCT(), push(new Boolean(true))),
                push(new Boolean(false))),
                Sequence(Expression(), swap(),
                        push(AggregatorFactory.createSample((Boolean) pop(), (Expr) pop()))),
                CLOSE_BRACE());
    }

    public Rule Avg() {
        return Sequence(AVG(), OPEN_BRACE(), FirstOf(
                Sequence(DISTINCT(), push(new Boolean(true))),
                push(new Boolean(false))),
                Sequence(Expression(), swap(),
                        push(AggregatorFactory.createAvg((Boolean) pop(), (Expr) pop()))),
                CLOSE_BRACE());
    }

    public Rule Max() {
        return Sequence(MAX(), OPEN_BRACE(), FirstOf(
                Sequence(DISTINCT(), push(new Boolean(true))),
                push(new Boolean(false))),
                Sequence(Expression(), swap(),
                        push(AggregatorFactory.createMax((Boolean) pop(), (Expr) pop()))),
                CLOSE_BRACE());
    }

    public Rule Min() {
        return Sequence(MIN(), OPEN_BRACE(), FirstOf(
                Sequence(DISTINCT(), push(new Boolean(true))),
                push(new Boolean(false))),
                Sequence(Expression(), swap(),
                        push(AggregatorFactory.createMin((Boolean) pop(), (Expr) pop()))),
                CLOSE_BRACE());
    }

    public Rule Sum() {
        return Sequence(SUM(), OPEN_BRACE(), FirstOf(
                Sequence(DISTINCT(), push(new Boolean(true))),
                push(new Boolean(false))),
                Sequence(Expression(), swap(),
                        push(AggregatorFactory.createSum((Boolean) pop(), (Expr) pop()))),
                CLOSE_BRACE());
    }

    public Rule Count() {
        return Sequence(COUNT(), OPEN_BRACE(), FirstOf(
                Sequence(DISTINCT(), push(new Boolean(true))),
                push(new Boolean(false))),
                FirstOf(
                        Sequence(ASTERISK(), push(AggregatorFactory.createCount((Boolean) pop()))),
                        Sequence(Expression(), swap(),
                                push(AggregatorFactory.createCountExpr((Boolean) pop(), (Expr) pop())))
                ),
                CLOSE_BRACE());
    }

    public Rule COUNT() {
        return StringIgnoreCaseWS("COUNT");
    }

    public Rule SUM() {
        return StringIgnoreCaseWS("SUM");
    }

    public Rule MIN() {
        return StringIgnoreCaseWS("MIN");
    }

    public Rule MAX() {
        return StringIgnoreCaseWS("MAX");
    }

    public Rule SAMPLE() {
        return StringIgnoreCaseWS("SAMPLE");
    }

    public Rule GROUP_CONCAT() {
        return StringIgnoreCaseWS("GROUP_CONCAT");
    }


    public Rule IF() {
        return IgnoreCase("IF");
    }

    public Rule STRLANG() {
        return IgnoreCase("STRLANG");
    }

    public Rule STRDT() {
        return IgnoreCase("STRDT");
    }

    public Rule SAME_TERM() {
        return IgnoreCase("SAME_TERM");
    }

    public Rule IS_NUMERIC() {
        return IgnoreCase("IS_NUMERIC");
    }

    public Rule ENCODE_FOR_URI() {
        return IgnoreCase("ENCODE_FOR_URI");
    }

    public Rule CONTAINS() {
        return IgnoreCase("CONTAINS");
    }

    public Rule SUBSTR() {
        return IgnoreCase("SUBSTR");
    }

    public Rule REPLACE() {
        return IgnoreCase("REPLACE");
    }

    public Rule STRLEN() {
        return IgnoreCase("STRLEN");
    }

    public Rule UCASE() {
        return IgnoreCase("UCASE");
    }

    public Rule LCASE() {
        return IgnoreCase("LCASE");
    }

    public Rule AVG() {
        return IgnoreCase("AVG");
    }

    public Rule ROUND() {
        return IgnoreCase("ROUND");
    }

    public Rule FLOOR() {
        return IgnoreCase("FLOOR");
    }

    public Rule CONCAT() {
        return IgnoreCase("CONCAT");
    }

    public Rule CEIL() {
        return IgnoreCase("CEIL");
    }

    public Rule RAND() {
        return IgnoreCase("RAND");
    }

    public Rule NIL() {
        return Sequence(LESS(), IgnoreCase("NIL"), GREATER());
    }

    public Rule RegexExpression() {
        return Sequence(REGEX(), OPEN_BRACE(), Expression(), COMMA(),
                Expression(), Optional(Sequence(COMMA(), Expression())),
                CLOSE_BRACE());
    }

    public Rule IriRefOrFunction() {
        return Sequence(IriRef(), push(new Function((Node_URI) pop())), Optional(Sequence(ArgList()
                , push(((Function) pop(1)).add((Args) pop())), FirstOf(addFunctionCall(), addAggregateFunctionCall())
        )));
    }

    public Rule RdfLiteral() {
        return Sequence(String(), push(NodeFactory.createLiteralByValue(trimMatch().replace("\"", ""), XSDDatatype.XSDstring)), Optional(FirstOf(LANGTAG(), Sequence(
                REFERENCE(), IriRef()))));
    }

    public Rule NumericLiteral() {
        return FirstOf(NumericLiteralUnsigned(), NumericLiteralPositive(),
                NumericLiteralNegative());
    }

    public String trimMatch() {
        return match().trim();
    }

    public Rule NumericLiteralUnsigned() {
        return FirstOf(
                Sequence(DOUBLE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdouble))),
                Sequence(DECIMAL(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdecimal))),
                Sequence(INTEGER(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDinteger))));
    }

    public boolean asExpr() {
        return push(ExprUtils.nodeToExpr((Node) pop()));
    }

    public Rule NumericLiteralPositive() {
        return FirstOf(
                Sequence(DOUBLE_POSITIVE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdouble))),
                Sequence(DECIMAL_POSITIVE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdecimal))),
                Sequence(INTEGER_POSITIVE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDinteger))));
    }

    public Rule NumericLiteralNegative() {
        return FirstOf(
                Sequence(DOUBLE_NEGATIVE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdouble))),
                Sequence(DECIMAL_NEGATIVE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdecimal))),
                Sequence(INTEGER_NEGATIVE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDinteger))));
    }

    public Rule BooleanLiteral() {
        return Sequence(FirstOf(TRUE(), FALSE()), push(NodeFactory.createLiteralByValue(trimMatch(), XSDDatatype.XSDboolean)));
    }

    public Rule String() {
        return FirstOf(STRING_LITERAL_LONG1(), STRING_LITERAL1(),
                STRING_LITERAL_LONG2(), STRING_LITERAL2());
    }

    public Rule IriRef() {
        return FirstOf(Sequence(IRI_REF(), push(NodeFactory.createURI(trimMatch().replace(">", "").replace("<", "")))), PrefixedName());
    }

    public Rule PrefixedName() {
        return FirstOf(Sequence(PNAME_LN(), resolvePNAME(match())), Sequence(PNAME_NS(), resolvePNAME(match())));
    }

    public boolean resolvePNAME(String match) {
        String uri = getQuery("BuiltInCall", -1).getQ().getPrologue().expandPrefixedName(match.trim());
        return push(NodeFactory.createURI(uri));
    }

    public Rule BlankNode() {
        return FirstOf(BLANK_NODE_LABEL(), Sequence(OPEN_SQUARE_BRACE(),
                CLOSE_SQUARE_BRACE()));
    }
    // </Parser>

    // <Lexer>

    public Rule WS() {
        return ZeroOrMore(FirstOf(COMMENT(), WS_NO_COMMENT()));
    }

    public Rule WS_NO_COMMENT() {
        return FirstOf(Ch(' '), Ch('\t'), Ch('\f'), EOL());
    }

    public Rule PNAME_NS() {
        return Sequence(Optional(PN_PREFIX()), ChWS(':'));
    }

    public Rule PNAME_LN() {
        return Sequence(PNAME_NS(), PN_LOCAL());
    }

    public Rule BASE() {
        return StringIgnoreCaseWS("BASE");
    }

    public Rule PREFIX() {
        return StringIgnoreCaseWS("PREFIX");
    }

    public Rule SELECT() {
        return StringIgnoreCaseWS("SELECT");
    }

    public Rule DISTINCT() {
        return StringIgnoreCaseWS("DISTINCT");
    }

    public Rule REDUCED() {
        return StringIgnoreCaseWS("REDUCED");
    }

    public Rule CONSTRUCT() {
        return StringIgnoreCaseWS("CONSTRUCT");
    }

    public Rule DESCRIBE() {
        return StringIgnoreCaseWS("DESCRIBE");
    }

    public Rule ASK() {
        return StringIgnoreCaseWS("ASK");

    }

    public Rule FROM() {
        return StringIgnoreCaseWS("FROM");
    }

    public Rule NAMED() {
        return StringIgnoreCaseWS("NAMED");
    }

    public Rule WHERE() {
        return StringIgnoreCaseWS("WHERE");
    }

    public Rule ORDER() {
        return StringIgnoreCaseWS("ORDER");
    }

    public Rule BY() {
        return StringIgnoreCaseWS("BY");
    }

    public Rule ASC() {
        return Sequence(StringIgnoreCaseWS("ASC"), push("ASC"));
    }

    public Rule DESC() {
        return Sequence(StringIgnoreCaseWS("DESC"), push("DESC"));
    }

    public Rule LIMIT() {
        return StringIgnoreCaseWS("LIMIT");
    }

    public Rule OFFSET() {
        return StringIgnoreCaseWS("OFFSET");
    }

    public Rule OPTIONAL() {
        return StringIgnoreCaseWS("OPTIONAL");
    }

    public Rule GRAPH() {
        return StringIgnoreCaseWS("GRAPH");
    }

    public Rule UNION() {
        return StringIgnoreCaseWS("UNION");
    }

    public Rule FILTER() {
        return StringIgnoreCaseWS("FILTER");
    }

    public Rule A() {
        return ChWS('a');
    }

    public Rule GROUP() {
        return StringIgnoreCaseWS("GROUP");
    }

    public Rule AS() {
        debug("AS");
        return StringIgnoreCaseWS("AS");
    }

    public Rule STR() {
        return StringIgnoreCaseWS("STR");
    }

    public Rule LANG() {
        return StringIgnoreCaseWS("LANG");
    }

    public Rule LANGMATCHES() {
        return StringIgnoreCaseWS("LANGMATCHES");
    }

    public Rule DATATYPE() {
        return StringIgnoreCaseWS("DATATYPE");
    }

    public Rule BOUND() {
        return StringIgnoreCaseWS("BOUND");
    }

    public Rule BNODE() {
        return StringIgnoreCaseWS("BNODE");
    }

    public Rule SAMETERM() {
        return StringIgnoreCaseWS("SAMETERM");
    }

    public Rule ISIRI() {
        return StringIgnoreCaseWS("ISIRI");
    }

    public Rule ISURI() {
        return StringIgnoreCaseWS("ISURI");
    }

    public Rule ISBLANK() {
        return StringIgnoreCaseWS("ISBLANK");
    }

    public Rule ISLITERAL() {
        return StringIgnoreCaseWS("ISLITERAL");
    }

    public Rule REGEX() {
        return StringIgnoreCaseWS("REGEX");
    }

    public Rule TRUE() {
        return StringIgnoreCaseWS("TRUE");
    }

    public Rule FALSE() {
        return StringIgnoreCaseWS("FALSE");
    }

    public Rule IRI_REF() {
        return Sequence(LESS_NO_COMMENT(), //
                ZeroOrMore(Sequence(TestNot(FirstOf(LESS_NO_COMMENT(), GREATER(), '"', OPEN_CURLY_BRACE(),
                        CLOSE_CURLY_BRACE(), '|', '^', '\\', '`', CharRange('\u0000', '\u0020'))), ANY)), //
                GREATER());
    }

    public Rule BLANK_NODE_LABEL() {
        return Sequence("_:", PN_LOCAL(), WS());
    }

    public Rule VAR1() {
        return Sequence('?', VARNAME(), WS());
    }

    public boolean allocVariable(String s) {
        return push(Var.alloc(s.trim().replace("?", "").replace("$", "")));
    }

    public Rule VAR2() {
        return Sequence('$', VARNAME(), WS());
    }

    public Rule LANGTAG() {
        return Sequence('@', OneOrMore(PN_CHARS_BASE()), ZeroOrMore(Sequence(
                MINUS(), OneOrMore(Sequence(PN_CHARS_BASE(), DIGIT())))), WS());
    }

    public Rule INTEGER() {
        return Sequence(OneOrMore(DIGIT()), WS());
    }

    public Rule DECIMAL() {
        return Sequence(FirstOf( //
                Sequence(Sequence(OneOrMore(DIGIT()), DOT(), ZeroOrMore(DIGIT())), push(NodeFactory.createLiteral(match(), XSDDatatype.XSDdecimal))), //
                Sequence(Sequence(DOT(), OneOrMore(DIGIT())), push(NodeFactory.createLiteral(match(), XSDDatatype.XSDdecimal))) //
        ), WS());


    }

    public Rule DOUBLE() {
        return Sequence(FirstOf(
                Sequence(OneOrMore(DIGIT()), DOT(), ZeroOrMore(DIGIT()), EXPONENT()),
                Sequence(DOT(), OneOrMore(DIGIT()), EXPONENT())),
                Sequence(OneOrMore(DIGIT()), EXPONENT()), WS());
    }

    public Rule INTEGER_POSITIVE() {
        return Sequence(PLUS(), INTEGER());
    }

    public Rule DECIMAL_POSITIVE() {
        return Sequence(PLUS(), DECIMAL());
    }

    public Rule DOUBLE_POSITIVE() {
        return Sequence(PLUS(), DOUBLE());
    }

    public Rule INTEGER_NEGATIVE() {
        return Sequence(MINUS(), INTEGER());
    }

    public Rule DECIMAL_NEGATIVE() {
        return Sequence(MINUS(), DECIMAL());
    }

    public Rule DOUBLE_NEGATIVE() {
        return Sequence(MINUS(), DOUBLE());
    }

    public Rule EXPONENT() {
        return Sequence(IgnoreCase('e'), Optional(FirstOf(PLUS(), MINUS())),
                OneOrMore(DIGIT()));
    }

    public Rule STRING_LITERAL1() {
        return Sequence("'", ZeroOrMore(FirstOf(Sequence(TestNot(FirstOf("'",
                '\\', '\n', '\r')), ANY), ECHAR())), "'", WS());
    }

    public Rule STRING_LITERAL2() {
        return Sequence('"', ZeroOrMore(FirstOf(Sequence(TestNot(AnyOf("\"\\\n\r")), ANY), ECHAR())), '"', WS());
    }

    public Rule STRING_LITERAL_LONG1() {
        return Sequence("'''", ZeroOrMore(Sequence(
                Optional(FirstOf("''", "'")), FirstOf(Sequence(TestNot(FirstOf(
                        "'", "\\")), ANY), ECHAR()))), "'''", WS());
    }

    public Rule STRING_LITERAL_LONG2() {
        return Sequence("\"\"\"", ZeroOrMore(Sequence(Optional(FirstOf("\"\"", "\"")),
                FirstOf(Sequence(TestNot(FirstOf("\"", "\\")), ANY), ECHAR()))), "\"\"\"", WS());
    }

    public Rule ECHAR() {
        return Sequence('\\', AnyOf("tbnrf\\\"\'"));
    }

    public Rule PN_CHARS_U() {
        return FirstOf(PN_CHARS_BASE(), '_');
    }

    public Rule VARNAME() {
        return Sequence(FirstOf(PN_CHARS_U(), DIGIT()),
                ZeroOrMore(
                        FirstOf(
                                PN_CHARS_U(),
                                DIGIT(), '\u00B7', CharRange('\u0300', '\u036F'),
                                CharRange('\u203F', '\u2040')))
                , WS());
    }


    public Rule PN_CHARS() {
        return FirstOf(MINUS(), DIGIT(), PN_CHARS_U(), '\u00B7',
                CharRange('\u0300', '\u036F'), CharRange('\u203F', '\u2040'));
    }

    public Rule PN_PREFIX() {
        return Sequence(PN_CHARS_BASE(), Optional(ZeroOrMore(FirstOf(PN_CHARS(), Sequence(DOT(), PN_CHARS())))));
    }

    public Rule PN_LOCAL() {
        return Sequence(FirstOf(PN_CHARS_U(), DIGIT()),
                Optional(ZeroOrMore(FirstOf(PN_CHARS(), Sequence(DOT(), PN_CHARS())))), WS());
    }

    public Rule PN_CHARS_BASE() {
        return FirstOf( //
                CharRange('A', 'Z'),//
                CharRange('a', 'z'), //
                CharRange('\u00C0', '\u00D6'), //
                CharRange('\u00D8', '\u00F6'), //
                CharRange('\u00F8', '\u02FF'), //
                CharRange('\u0370', '\u037D'), //
                CharRange('\u037F', '\u1FFF'), //
                CharRange('\u200C', '\u200D'), //
                CharRange('\u2070', '\u218F'), //
                CharRange('\u2C00', '\u2FEF'), //
                CharRange('\u3001', '\uD7FF'), //
                CharRange('\uF900', '\uFDCF'), //
                CharRange('\uFDF0', '\uFFFD') //
        );
    }

    public Rule DIGIT() {
        return CharRange('0', '9');
    }

    public Rule COMMENT() {
        return Sequence('#', ZeroOrMore(Sequence(TestNot(EOL()), ANY)), EOL());
    }

    public Rule EOL() {
        return AnyOf("\n\r");
    }

    public Rule REFERENCE() {
        return StringWS("^^");
    }

    public Rule LESS_EQUAL() {
        return StringWS("<=");
    }

    public Rule GREATER_EQUAL() {
        return StringWS(">=");
    }

    public Rule NOT_EQUAL() {
        return StringWS("!=");
    }

    public Rule AND() {
        return StringWS("&&");
    }

    public Rule OR() {
        return StringWS("||");
    }

    public Rule OPEN_BRACE() {
        return ChWS('(');
    }

    public Rule CLOSE_BRACE() {
        return ChWS(')');
    }

    public Rule OPEN_CURLY_BRACE() {
        return ChWS('{');
    }

    public Rule CLOSE_CURLY_BRACE() {
        return ChWS('}');
    }

    public Rule OPEN_SQUARE_BRACE() {
        return ChWS('[');
    }

    public Rule CLOSE_SQUARE_BRACE() {
        return ChWS(']');
    }

    public Rule SEMICOLON() {
        return ChWS(';');
    }

    public Rule DOT() {
        return ChWS('.');
    }

    public Rule PLUS() {
        return ChWS('+');
    }

    public Rule MINUS() {
        return ChWS('-');
    }

    public Rule ASTERISK() {
        return ChWS('*');
    }

    public Rule COMMA() {
        return ChWS(',');
    }

    public Rule NOT() {
        return ChWS('!');
    }

    public Rule DIVIDE() {
        return ChWS('/');
    }

    public Rule EQUAL() {
        return ChWS('=');
    }

    public Rule LESS_NO_COMMENT() {
        return Sequence(Ch('<'), ZeroOrMore(WS_NO_COMMENT()));
    }

    public Rule LESS() {
        return ChWS('<');
    }

    public Rule GREATER() {
        return ChWS('>');
    }
    // </Lexer>

    public Rule ChWS(char c) {
        return Sequence(Ch(c), WS());
    }

    public Rule StringWS(String s) {
        return Sequence(String(s), WS());
    }

    public Rule StringIgnoreCaseWS(String string) {
        return Sequence(IgnoreCase(string), WS());
    }

    void debug(String calls) {
        System.out.println(calls);
    }
}