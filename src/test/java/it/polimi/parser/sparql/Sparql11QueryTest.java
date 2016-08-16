package it.polimi.parser.sparql;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.QueryCompare;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.parboiled.Parboiled;
import org.parboiled.errors.ParseError;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.commons.io.FileUtils.readFileToString;
import static org.junit.Assert.assertEquals;

/**
 * Created by Riccardo on 09/08/16.
 */
@RunWith(Parameterized.class)
public class Sparql11QueryTest {

    private static boolean res;
    private static String f;
    private static final String folder = "tests/";

    @Parameters
    public static Collection<Object[]> data() throws IOException {
        List<Object[]> obj = new ArrayList<Object[]>();
        for (String d : IOUtils.readLines(Sparql11QueryTest.class.getClassLoader()
                .getResourceAsStream(folder), Charsets.UTF_8)) {
            if (!d.contains("class")) {
                for (String f : IOUtils.readLines(Sparql11QueryTest.class.getClassLoader()
                        .getResourceAsStream(folder + d + "/"), Charsets.UTF_8)) {
                    if (!f.contains(".arq") && !f.contains(".sh") && (!f.contains("false") || !f.contains("bad"))) {
                        obj.add(new Object[]{(folder + d + "/" + f), true});
                    }
                }
            }
        }
        return obj;
    }

    public Sparql11QueryTest(String f, boolean res) {
        this.res = res;
        this.f = f;
    }

    static String input;
    static org.apache.jena.query.Query toCompare;

    @Before
    public void load() throws URISyntaxException, IOException {


    }

    @Test
    public void test() {
        try {
            (new Sparql11QueryTest(f, res)).process();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (QueryParseException epe){
            epe.printStackTrace();
        }
    }

    public static void process() throws URISyntaxException, IOException {
        System.out.println(f);
        input = readFileToString(new File(Sparql11QueryTest.class.getClassLoader().getResource(f).toURI()));
        System.out.println(input);
        toCompare = QueryFactory.create(input);

        SPARQL11Parser parser = Parboiled.createParser(SPARQL11Parser.class);
        parser.setResolver(IRIResolver.create());
        ReportingParseRunner reportingParseRunner = new ReportingParseRunner(parser.Query());
        ParsingResult<Query> result = reportingParseRunner.run(input);
        if (result.hasErrors()) {
            for (ParseError e : result.parseErrors) {
                System.out.println(e.getStartIndex());
                System.out.println(e.getEndIndex());

                System.out.println(input.substring(0, e.getStartIndex()));
                System.err.print(input.substring(e.getStartIndex(), e.getEndIndex()));
                System.out.println(input.substring(e.getEndIndex(), input.length() - 1));

            }
        }
        org.apache.jena.query.Query q = result.parseTreeRoot.getChildren().get(0).getValue().getQ();
        QueryCompare.PrintMessages = true;
        assertEquals(res, org.apache.jena.sparql.core.QueryCompare.equals(toCompare, q));
    }


}
