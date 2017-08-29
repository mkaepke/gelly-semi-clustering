import de.haw.PrepareHelper.EdgeNormalizer;
import de.haw.PrepareHelper.LoopFilter;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SemiClusteringTests {

    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @Before
    public void setUp() {

    }

    @Test
    public void testPrepareEdeList() throws Exception {
        List<Tuple3<Double, Double, Double>> edgeList = new ArrayList<>();
        edgeList.add(Tuple3.of(1.0, 2.0, 1.0));
        edgeList.add(Tuple3.of(1.0, 1.0, 1.0));
        edgeList.add(Tuple3.of(2.0, 1.0, 1.0));
        DataSet<Tuple3<Double, Double, Double>> edgeDataSet = env.fromCollection(edgeList);

        DataSet<Tuple3<Double, Double, Double>> result = edgeDataSet
              .filter(new LoopFilter())
              .name("remove self loops")
              .flatMap(new EdgeNormalizer())
              .name("reverse edges => srcID < trgID")
              .groupBy(0, 1)
              .sum(2)
              .name("combine multi edges");

        List<Tuple3<Double, Double, Double>> testTmp = new ArrayList<>();
        testTmp.add(Tuple3.of(1.0, 2.0, 2.0));
        DataSet<Tuple3<Double, Double, Double>> expectedResult = env.fromCollection(testTmp);

        Assert.assertEquals(expectedResult.count(), result.count());
        Assert.assertEquals(expectedResult.collect().get(0).f0, result.collect().get(0).f0);
        Assert.assertEquals(expectedResult.collect().get(0).f1, result.collect().get(0).f1);
        Assert.assertEquals(expectedResult.collect().get(0).f2, result.collect().get(0).f2);
    }
}
