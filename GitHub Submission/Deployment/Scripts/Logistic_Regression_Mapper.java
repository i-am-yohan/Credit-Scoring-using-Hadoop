import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Logistic_Regression_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public static int count=0;
    public static long number_inputs=(long) 0;
    public static float alpha=0.0f;
    public static Float[] Xi=null;
    public static ArrayList<Float> theta_i=new ArrayList<Float>();
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        alpha=context.getConfiguration().getFloat("alpha",0);
        number_inputs=context.getConfiguration().getInt("n_rows",0);
        //number_inputs=context.getCounter(org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS).getValue();
    }
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ++count;
        float h_theta=0;
        String[] tok0=value.toString().split("\t");
        String[] tok = Arrays.copyOfRange(tok0 , 1 , tok0.length);
        if(count==1) {
            for (int i = 0; i < tok.length; i++) {
                theta_i.add(context.getConfiguration().getFloat("theta".concat(String.valueOf(i)), 0));
            }
            Xi = new Float[tok.length];
        }
        for(int i=0;i<Xi.length;i++) {
            if (i == 0) {
                Xi[0] = (float) 1;
            } else {
                Xi[i] = Float.parseFloat(tok[i - 1]);
            }
        }
        for(int i=0;i<Xi.length;i++){
            float exp=0;
            exp+=(Xi[i]*theta_i.get(i));
            //If you choose to use perceptron learning rule
			/*if(i==5){
				if(exp>=0){
					h_theta=1;
				}
				else{
					h_theta=0;
				}
			}*/
            //If you choose to use the Logistic Function for learning
            if(i==(Xi.length-1)){
                h_theta=(float) (1/(1+(Math.exp(-(exp)))));
            }
        }
        float Yi=Float.parseFloat(tok[tok.length-1]);
        for(int i=0;i<Xi.length;i++){
            float temp=theta_i.get(i);
            theta_i.remove(i);
            theta_i.add(i,(float) (temp-(alpha/ (float) number_inputs)*(h_theta-Yi)*(Xi[i])));
        }
        //System.out.println(theta_i);
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        for(int i=0;i<theta_i.size();i++){
            context.write(new Text("theta"+i), new FloatWritable(theta_i.get(i)));
        }
    }
}