import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author shenjiang
 * @Description:
 * @Date: 2020/6/2 20:30
 */
public class SparkWordCount {
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("WordCountCluster");
        //第二步
        JavaSparkContext sc = new JavaSparkContext(conf);
        //获取hdfs输入
        JavaRDD<String> lines = sc.textFile("hdfs://hadoop101:9000/user/atguigu/input/README.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

            /**
             * 返回元素以“ ”切割的迭代器的
             * @param line
             * @return
             * @throws Exception
             */
            @Override
            public Iterator<String> call(String line) throws Exception {
                List<String> strings = Arrays.asList(line.split(" "));
                return strings.iterator();
            }

            private static final long serialVersionUID = 1L;


        });

        /**
         * JavaPairRDD<K2,V2> mapToPair(PairFunction<T,K2,V2> f)
         *
         * 此函数会对一个RDD中的每个元素调用f函数，其中原来RDD中的每一个元素都是T类型的，调用f函数后会进行一定的操作把每个元素都转换成一个<K2,V2>类型的对象
         */
        JavaPairRDD<String,Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {

                    private  static final long serialVersionUID = 1L;

                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word,1);
                    }
                }
        );


        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        );


        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" : "+ wordCount._2 );
            }
        });

        sc.close();

    }
}
