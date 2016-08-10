package sparql;

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
import org.parboiled.Rule;

/**
 * Created by Riccardo on 09/08/16.
 */
public class SPARQL11Parser extends SPARQL11Lexer {

    public Rule Query() {
        return Sequence(push(new Query(getResolver())), WS(), Prologue(), FirstOf(SelectQuery(), ConstructQuery(), AskQuery(), DescribeQuery())
                , EOI);
    }

    public Rule Prologue() {
        return Sequence(Optional(BaseDecl()), ZeroOrMore(PrefixDecl()));
    }

    public Rule BaseDecl() {
        return Sequence(BASE(), IRI_REF(), pushQuery(((Query) pop(0)).setBaseURI(trimMatch().replace(">", "").replace("<", ""))), WS());
    }

    public Rule PrefixDecl() {
        return Sequence(PrefixBuild(), pushQuery(((Query) pop(1)).setPrefix((Prefix) pop())), WS());
    }

    public Rule PrefixBuild() {
        return Sequence(PREFIX(), PNAME_NS(), push(new Prefix(trimMatch())), IRI_REF(), push(((Prefix) pop()).setURI(URIMatch())), WS());
    }

    public Rule SelectQuery() {
        return Sequence(
                SelectClause(),
                ZeroOrMore(DatasetClause()),
                WhereClause(),
                SolutionModifiers());
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

    public Rule DescribeQuery() {
        return Sequence(Sequence(DESCRIBE(), pushQuery(popQuery(0).setDescribeQuery())), FirstOf(OneOrMore(Sequence(VarOrIRIref(), push(popQuery(1).addDescribeNode((Node) pop())))),
                Sequence(ASTERISK(), push(popQuery(0).setQueryStar()))), ZeroOrMore(DatasetClause()),
                Optional(WhereClause()), SolutionModifiers());
    }

    public Rule AskQuery() {
        return Sequence(Sequence(ASK(), pushQuery(popQuery(0).setAskQuery())), ZeroOrMore(DatasetClause()), WhereClause());
    }

    public Rule SelectClause() {
        debug("SelectClause");
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
                                                pushQuery(((Query) pop(2)).addResultVar((Node) pop(), (Expr) pop())))))));
    }


    public Rule ConstructWhereClause() {
        return Sequence(CONSTRUCT(), pushQuery(popQuery(0).setConstructQuery()), WHERE(), OPEN_CURLY_BRACE(), pushQuery(popQuery(0).setConstructQuery()), push(new ElementGroup()), TriplesTemplate(), addTemplateAndPatternToQuery(), addElementToQuery(), CLOSE_CURLY_BRACE());
    }

    public Rule ConstructClause() {
        return Sequence(CONSTRUCT(), pushQuery(popQuery(0).setConstructQuery()), ConstructTemplate(), addTemplateToQuery());
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

    public Rule WhereClause() {
        return Sequence(Optional(WHERE()), GroupGraphPattern(), addElementToQuery());
    }

    public Rule SolutionModifiers() {
        return Sequence(Optional(GroupClause()), Optional(HavingClause()), Optional(OrderClause()), Optional(LimitOffsetClauses()));
    }

    public Rule GroupClause() {
        return Sequence(GROUP(), BY(), OneOrMore(GroupCondition()));
    }

    public Rule HavingClause() {
        return Sequence(HAVING(), OneOrMore(Constraint(), pushQuery(popQuery(1).addHavingCondition((Expr) pop()))));
    }

    public Rule OrderClause() {
        return Sequence(ORDER(), BY(), OneOrMore(OrderCondition()));
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
        return Sequence(OPEN_CURLY_BRACE(), FirstOf(SubSelect(), GroupGraphPatternSub()), CLOSE_CURLY_BRACE());
    }


    public Rule SubSelect() {
        return Sequence(startSubQuery(-1), SelectClause(), WhereClause(), SolutionModifiers(), endSubQuery());
    }

    public boolean startSubQuery(int i) {
        return push(new Query(getQuery(i).getQ().getPrologue()));
    }

    public boolean endSubQuery() {
        return push(new ElementSubQuery(popQuery(0).getQ()));
    }

    public Rule GroupGraphPatternSub() {
        debug("GroupGraphPatternSub");
        return Sequence(push(new ElementGroup()), Optional(TriplesBlock(), addSubElement()),
                ZeroOrMore(GraphPatternNotTriples(), addSubElement(), Optional(DOT()),
                        Optional(TriplesBlock(), addSubElement())));
    }


    public Rule GroupCondition() {
        return FirstOf(Sequence(Var(), pushQuery(((Query) pop(1)).addGroupBy((Var) pop()))),
                Sequence(BuiltInCall(), pushQuery(((Query) pop(1)).addGroupBy((Expr) pop()))),
                Sequence(FunctionCall(), pushQuery(((Query) pop(1)).addGroupBy((Expr) pop()))),
                Sequence(OPEN_BRACE(), Expression(), AS(), Var(), CLOSE_BRACE(), pushQuery(((Query) pop(2)).addGroupBy((Var) pop(), (Expr) pop())))
        );
    }

    public Rule OrderCondition() {
        return FirstOf(
                Sequence(FirstOf(ASC(), DESC()), BrackettedExpression(), pushQuery(((Query) pop(2)).addOrderBy((Expr) pop(), pop().toString()))),
                Sequence(FirstOf(Constraint(), Var()), pushQuery(((Query) pop(1)).addOrderBy(pop()))));
    }


    public Rule SourceSelector() {
        return IriRef();
    }


    public Rule GraphPatternNotTriples() {
        debug("GraphPatternNotTriples");
        return FirstOf(GroupOrUnionGraphPattern(), OptionalGraphPattern(), GraphGraphPattern(), Filter());
    }

    public Rule Filter() {
        return Sequence(FILTER(), FilterConstraint(), addFilterElement());
    }

    public Rule OptionalGraphPattern() {
        return Sequence(OPTIONAL(), GroupGraphPattern(), addOptionalElement());
    }

    public Rule GraphGraphPattern() {
        return Sequence(GRAPH(), VarOrIRIref(), GroupGraphPattern(), swap(), addNamedGraphElement());
    }

    public Rule GroupOrUnionGraphPattern() {
        return FirstOf(UnionGraphPattern(), GroupGraphPattern());
    }

    public Rule UnionGraphPattern() {
        return Sequence(createUnionElement(), GroupGraphPattern(), addUnionElement(),
                OneOrMore(UNION(), GroupGraphPattern(), addUnionElement()));
    }

    public Rule FilterConstraint() {
        return FirstOf(BrackettedExpression(), BuiltInCallNoAggregates(), FunctionCall());
    }

    public Rule Constraint() {
        return FirstOf(BrackettedExpression(), BuiltInCall(), FunctionCall());
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


    public Rule TriplesTemplate() {
        return Sequence(push(new TripleCollectorBGP()), TriplesTemplateSub());
    }

    public Rule TriplesTemplateSub() {
        return Sequence(TriplesSameSubject(), Optional(Sequence(DOT(),
                Optional(TriplesTemplateSub()))));
    }

    public Rule TriplesBlock() {
        return Sequence(push(new ElementPathBlock()), TriplesBlockSub());
    }

    public Rule TriplesBlockSub() {
        return Sequence(TriplesSameSubject(),
                Optional(DOT(), Optional(TriplesBlockSub())));
    }


    public Rule TriplesSameSubject() {
        return FirstOf(
                Sequence(Subj(), PropertyListNotEmpty(), drop()),
                Sequence(TriplesNode(), PropertyList(), drop()));
    }


    public Rule ConstructTemplate() {
        return Sequence(OPEN_CURLY_BRACE(), push(new TripleCollectorBGP()), ConstructTriples(), CLOSE_CURLY_BRACE());
    }

    public Rule ConstructTriples() {
        return Sequence(TriplesSameSubject(), Optional(Sequence(DOT(), Optional(ConstructTriples()))));
    }


    public Rule PropertyListNotEmpty() {
        return Sequence(Sequence(Verb(), ObjectList(), drop())
                , ZeroOrMore(Sequence(SEMICOLON(),
                        Optional(Sequence(Verb(), ObjectList()))), drop()));
    }

    public Rule PropertyList() {
        return Optional(PropertyListNotEmpty());
    }

    public Rule ObjectList() {
        return Sequence(Object_(), addTripleToBloc(((TripleCollector) peek(3))),
                ZeroOrMore(Sequence(COMMA(), Object_(), addTripleToBloc(((TripleCollector) peek(3))))));
    }


    public Rule Object_() {
        return GraphNode();
    }

    public Rule Subj() {
        return VarOrTerm();
    }

    public Rule Verb() {
        return FirstOf(VarOrIRIref(), Sequence(A(), push(nRDFtype)));
    }

    public Rule TriplesNode() {
        return FirstOf(Collection(), BlankNodePropertyList());
    }

    public Rule BlankNodePropertyList() {
        return Sequence(OPEN_SQUARE_BRACE(), PropertyListNotEmpty(),
                CLOSE_SQUARE_BRACE());
    }

    public Rule Collection() {
        return Sequence(OPEN_BRACE(), OneOrMore(GraphNode()), CLOSE_BRACE());
    }

    public Rule GraphNode() {
        return FirstOf(VarOrTerm(), TriplesNode());
    }

    public Rule VarOrTerm() {
        return FirstOf(Var(), GraphTerm());
    }

    public Rule Var() {
        return Sequence(FirstOf(VAR1(), VAR2()), allocVariable(trimMatch()));
    }

    public Rule VarOrIRIref() {
        return FirstOf(Var(), IriRef());
    }

    public Rule GraphTerm() {
        return FirstOf(IriRef(), RdfLiteral(), NumericLiteral(),
                BooleanLiteral(), BlankNode(), Sequence(OPEN_BRACE(),
                        CLOSE_BRACE()));
    }

    public Rule ExpressionList() {
        return Sequence(OPEN_BRACE(), Expression(), push(new ExprList((Expr) pop())), ZeroOrMore(Sequence(COMMA(), Expression(), addExprToExprList())), CLOSE_BRACE());
    }


    public Rule Expression() {
        return ConditionalOrExpression();
    }

    public Rule ConditionalOrExpression() {
        return Sequence(ConditionalAndExpression(), ZeroOrMore(Sequence(OR(),
                ConditionalAndExpression()), swap(), push(new E_LogicalOr((Expr) pop(), (Expr) pop()))));
    }

    public Rule ConditionalAndExpression() {
        return Sequence(ValueLogical(), ZeroOrMore(Sequence(AND(),
                ValueLogical(), swap(), push(new E_LogicalAnd((Expr) pop(), (Expr) pop())))));
    }

    public Rule ValueLogical() {
        return RelationalExpression();
    }

    public Rule RelationalExpression() {
        return Sequence(NumericExpression(), Optional(FirstOf(
                Sequence(EQUAL(), NumericExpression(), swap(), push(new E_Equals((Expr) pop(), (Expr) pop()))),
                Sequence(NOT_EQUAL(), NumericExpression(), swap(), push(new E_NotEquals((Expr) pop(), (Expr) pop()))),
                Sequence(LESS(), NumericExpression(), swap(), push(new E_LessThan((Expr) pop(), (Expr) pop()))),
                Sequence(GREATER(), NumericExpression(), swap(), push(new E_GreaterThan((Expr) pop(), (Expr) pop()))),
                Sequence(LESS_EQUAL(), NumericExpression(), swap(), push(new E_LessThanOrEqual((Expr) pop(), (Expr) pop()))),
                Sequence(GREATER_EQUAL(), NumericExpression(), swap(), push(new E_GreaterThanOrEqual((Expr) pop(), (Expr) pop())))
        )));
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
        return FirstOf(
                Sequence(NOT(), PrimaryExpression(), push(new E_LogicalNot((Expr) pop()))),
                Sequence(PLUS(), PrimaryExpression()),
                Sequence(MINUS(), PrimaryExpression()),
                PrimaryExpression());
    }

    public Rule PrimaryExpression() {
        return FirstOf(
                BrackettedExpression(),
                BuiltInCall(),
                IriRefOrFunction(),
                Sequence(RdfLiteral(), asExpr()),
                Sequence(NumericLiteral(), asExpr()),
                Sequence(BooleanLiteral(), asExpr()),
                Sequence(Var(), asExpr()));
    }

    public Rule BrackettedExpression() {
        return Sequence(OPEN_BRACE(), Expression(), CLOSE_BRACE());
    }

    public Rule BuiltInCall() {
        return FirstOf(
                Sequence(Aggregate(), push(getQuery(1).allocAggregate((Aggregator) pop()))),
                BuiltInCallNoAggregates()
        );
    }

    public Rule BuiltInCallNoAggregates() {
        return FirstOf(
                //TODO verify is the are all

                Sequence(STR(), OPEN_BRACE(), Expression(), push(new E_Str((Expr) pop())), CLOSE_BRACE()),
                Sequence(LANG(), OPEN_BRACE(), Expression(), push(new E_Lang((Expr) pop())), CLOSE_BRACE()),
                Sequence(LANGMATCHES(), OPEN_BRACE(), Expression(), COMMA(),
                        Expression(), swap(), push(new E_LangMatches((Expr) pop(), (Expr) pop())), CLOSE_BRACE()),
                Sequence(DATATYPE(), OPEN_BRACE(), Expression(), push(new E_Datatype((Expr) pop())), CLOSE_BRACE()),
                Sequence(BOUND(), OPEN_BRACE(), Var(), push(new E_Bound(new ExprVar((Var) pop()))), CLOSE_BRACE()),
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
                Sequence(LCASE(), OPEN_BRACE(), Expression(), push(new E_StrLowerCase((Expr) pop())), CLOSE_BRACE()),
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


    public Rule RegexExpression() {
        return Sequence(REGEX(), OPEN_BRACE(), Expression(), COMMA(),
                Expression(), Optional(Sequence(COMMA(), Expression())),
                CLOSE_BRACE());
    }

    public Rule IriRefOrFunction() {
        return FirstOf(FunctionCall(), Sequence(IriRef(), asExpr()));
    }

    public Rule FunctionCall() {
        return Sequence(IriRef(), ArgList(), push(new Function((Args) pop(), (Node_URI) pop())),
                FirstOf(addFunctionCall(), addAggregateFunctionCall()));
    }

    public Rule addAggregateFunctionCall() {
        return Sequence(Test((AggregateRegistry.isRegistered(((Function) peek()).getIri()))),
                push(getQuery(1).allocAggregate(((Function) pop()).createCustom())));
    }


    public Rule ArgList() {
        return Sequence(push(new Args()),
                FirstOf(Sequence(OPEN_BRACE(), CLOSE_BRACE()),
                        Sequence(OPEN_BRACE(), Expression(), addArg(), ZeroOrMore(Sequence(COMMA(),
                                Expression(), addArg())), CLOSE_BRACE())));
    }

    public Rule RdfLiteral() {
        return Sequence(String(), push(trimMatch().replace("\"", "").replace("\'", "")),
                FirstOf(
                        Sequence(LANGTAG(), push(NodeFactory.createLiteral(pop().toString(), trimMatch().substring(1)))),
                        Sequence(REFERENCE(), IriRef(), swap(),
                                push(NodeFactory.createLiteral(pop().toString(),
                                        getSafeTypeByName(((Node_URI) pop()).getURI()))))
                        , push(NodeFactory.createLiteral(pop().toString()))));
    }


    public Rule NumericLiteral() {
        return FirstOf(NumericLiteralUnsigned(), NumericLiteralPositive(),
                NumericLiteralNegative());
    }

    public Rule NumericLiteralUnsigned() {
        return FirstOf(
                Sequence(DOUBLE(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdouble))),
                Sequence(DECIMAL(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDdecimal))),
                Sequence(INTEGER(), push(NodeFactory.createLiteral(trimMatch(), XSDDatatype.XSDinteger))));
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
        return Sequence(FirstOf(TRUE(), FALSE()), push(NodeFactory.createLiteralByValue(trimMatch(), XSDDatatype.XSDboolean))
                , Optional(REFERENCE(), IriRef(), drop()));
    }

    public Rule String() {
        return FirstOf(STRING_LITERAL_LONG1(), STRING_LITERAL1(),
                STRING_LITERAL_LONG2(), STRING_LITERAL2());
    }

    public Rule IriRef() {
        return Sequence(FirstOf(
                Sequence(IRI_REF(), push(URIMatch())),
                Sequence(PrefixedName(), push(resolvePNAME(URIMatch())))),
                push(NodeFactory.createURI(pop().toString())));
    }

    public Rule PrefixedName() {
        return FirstOf(PNAME_LN(), PNAME_NS());
    }


    public Rule BlankNode() {
        return FirstOf(BLANK_NODE_LABEL(), Sequence(OPEN_SQUARE_BRACE(),
                CLOSE_SQUARE_BRACE()));
    }


}