package sparql;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.parboiled.Parboiled;
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
    private static final String folder = "sparql/";

    @Parameters
    public static Collection<Object[]> data() throws IOException {
        List<Object[]> obj = new ArrayList<Object[]>();
        for (String d : IOUtils.readLines(Sparql11QueryTest.class.getClassLoader()
                .getResourceAsStream(folder), Charsets.UTF_8)) {
            if (!d.contains("class")) {
                for (String f : IOUtils.readLines(Sparql11QueryTest.class.getClassLoader()
                        .getResourceAsStream(folder + d + "/"), Charsets.UTF_8)) {
                    if (!f.contains(".arq")) {
                        obj.add(new Object[]{(folder + d + "/" + f), !f.contains("false")});
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


    @Test
    public void test() throws URISyntaxException, IOException {
        (new Sparql11QueryTest(f, res)).process();
    }

    public static void process() throws IOException, URISyntaxException {
        System.out.println(f);
        String input = readFileToString(new File(Sparql11QueryTest.class.getClassLoader().getResource(f).toURI()));
        System.out.println(input);

        SPARQL11Parser parser = Parboiled.createParser(SPARQL11Parser.class);
        ReportingParseRunner reportingParseRunner = new ReportingParseRunner(parser.Query());
        ParsingResult<Query> result = reportingParseRunner.run(input);
        org.apache.jena.query.Query q = result.parseTreeRoot.getChildren().get(0).getValue().getQ();
        org.apache.jena.query.Query query1 = QueryFactory.create(input);
        assertEquals(res, org.apache.jena.sparql.core.QueryCompare.equals(query1, q));
    }


}
