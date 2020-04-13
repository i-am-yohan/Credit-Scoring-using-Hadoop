import java.io.IOException;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.ArrayList;


//Explore the data
public class DIA_Project_Development {

    //Gathering all of the totals - important for default rate and WoE Calculation
    public static class All_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("All"), new Text(record1));
        }
    }

    //The reducer used to calculate the required max, mins and totals
    public static class All_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //Initializing the cumulative totals and the max' as 0
            int Total = 0;
            int Bads = 0;
            int Goods = 0;
            double LTV_MAX = 0.0;
            double AGE_MAX = 0.0;
            double INTSLMT_AMT_MAX = 0.0;
            //INitializing the minimums at an absurdly high value
            double LTV_MIN = 10000000.0;
            double AGE_MIN = 10000000.0;
            double INTSLMT_AMT_MIN = 10000000.0;
            for (Text val : values) {
                String[] vec = val.toString().split(",");
                if (!vec[0].equals("ID")){
                    Total++;
                    Bads = Bads + Integer.parseInt(vec[1]);
                    LTV_MAX = Double.max(LTV_MAX, Double.parseDouble(vec[3]));
                    LTV_MIN = Double.min(LTV_MIN, Double.parseDouble(vec[3]));
                    AGE_MAX = Double.max(AGE_MAX, Double.parseDouble(vec[5]));
                    AGE_MIN = Double.min(AGE_MIN, Double.parseDouble(vec[5]));
                    INTSLMT_AMT_MAX = Double.max(INTSLMT_AMT_MAX, Double.parseDouble(vec[4]));
                    INTSLMT_AMT_MIN = Double.min(INTSLMT_AMT_MIN, Double.parseDouble(vec[4]));
                }
            }
            Goods = Total - Bads;
            String Out_STR1 = String.format("%d\t%d\t%d\t%f\t%f\t%f\t%f\t%f\t%f", Total, Bads, Goods, LTV_MAX, LTV_MIN, AGE_MAX, AGE_MIN, INTSLMT_AMT_MAX, INTSLMT_AMT_MIN);
            context.write(key, new Text(Out_STR1));
            }
    }

    //For the grouping programs above - there's only one line in the table?
    public static class Portfolio_Total_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("Mortgage"), new Text("Total\t" + record1));
            context.write(new Text("Vehicle_Loan"), new Text("Total\t" + record1));
        }
    }

    //Group by Portfolio - mapper
    public static class Portfolio_Grouping_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {

            String record2 = value.toString();
            String[] tokens = record2.split(",");
            String Portfolio = tokens[2].replace(" ","_");
            if (!Portfolio.equals("PORTFOLIO")) {
                String DF_Ind = tokens[1];
                context.write(new Text(Portfolio) , new Text("DF\t" + DF_Ind));
            }
        }
    }

    //LTV Grouping - Results in unintutive grouping due to
    public static class LTV_Grouping_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {

            String record2 = value.toString();
            String[] tokens = record2.split(",");
            String LTV_Bucket = "";
            if (!tokens[0].equals("ID")) {
                Double LTV = Double.valueOf(tokens[3]);
                if (LTV <= 0.8488) {LTV_Bucket = "LTV_1_LT_85";}
                else if (LTV <= 1){LTV_Bucket = "LTV_2_LT_100";}
                else {LTV_Bucket = "LTV_3_GT100";}
                String DF_Ind = tokens[1];
                context.write(new Text(LTV_Bucket) , new Text("DF\t" + DF_Ind));
            }
        }
    }
    //A few mappers exist like this throughout the code. This is for joining purposes
    public static class LTV_Total_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("LTV_1_LT_85"), new Text("Total\t" + record1));
            context.write(new Text("LTV_2_LT_100"), new Text("Total\t" + record1));
            context.write(new Text("LTV_3_GT100"), new Text("Total\t" + record1));
        }
    }

    //Interest Only Grouping
    public static class IO_Grouping_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {

            String record2 = value.toString();
            String[] tokens = record2.split(",");
            String IO_Bucket = "";
            if (!tokens[0].equals("ID")) {
                Double Instl = Double.valueOf(tokens[4]);
                if (Instl < 750) {IO_Bucket = "Instl_LT_750";}
                else {IO_Bucket = "Instl_GT_750";}
                String DF_Ind = tokens[1];
                context.write(new Text(IO_Bucket) , new Text("DF\t" + DF_Ind));
            }
        }
    }
    public static class IO_Total_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("Instl_LT_750"), new Text("Total\t" + record1));
            context.write(new Text("Instl_GT_750"), new Text("Total\t" + record1));
        }
    }

    //AGE Grouping
    public static class AGE_Grouping_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {

            String record2 = value.toString();
            String[] tokens = record2.split(",");
            String AGE_Bucket = "";
            if (!tokens[0].equals("ID")) {
                Double Age = Double.valueOf(tokens[5]);
                if (Age <= 35) {AGE_Bucket = "AGE_LT_35";}
                else if (Age <= 55) {AGE_Bucket = "AGE_LT_55";}
                else {AGE_Bucket = "AGE_GT_55";}
                String DF_Ind = tokens[1];
                context.write(new Text(AGE_Bucket) , new Text("DF\t" + DF_Ind));
            }
        }
    }
    public static class AGE_Total_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("AGE_LT_35"), new Text("Total\t" + record1));
            context.write(new Text("AGE_LT_55"), new Text("Total\t" + record1));
            context.write(new Text("AGE_GT_55"), new Text("Total\t" + record1));
        }
    }

    //Protfolio Grouping and LTV
    public static class PortfolioLTV_Grouping_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {

            String record2 = value.toString();
            String[] tokens = record2.split(",");
            String PORTF_LTV_Bucket = "";
            if (!tokens[0].equals("ID")) {
                Double LTV = Double.valueOf(tokens[3]);
                String Portfolio = tokens[2];
                if (Portfolio.equals("Mortgage")){
                    if (LTV <= 1.132) {PORTF_LTV_Bucket = "MTG_LTV_1_LT_113";}
                    else if (LTV <= 1.198) {PORTF_LTV_Bucket = "MTG_LTV_2_LT_120";}
                    else {PORTF_LTV_Bucket = "MTG_LTV_3_GT_120";}
                }
                else {
                    if (LTV <= 0.6635) {PORTF_LTV_Bucket = "VEH_LTV_1_LT_66";}
                    else if (LTV <= 0.7841) {PORTF_LTV_Bucket = "VEH_LTV_2_LT_78";}
                    else {PORTF_LTV_Bucket = "VEH_LTV_3_GT_78";}
                }
                String DF_Ind = tokens[1];
                context.write(new Text(PORTF_LTV_Bucket) , new Text("DF\t" + DF_Ind));
            }
        }
    }
    public static class PortfolioLTV_Total_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("MTG_LTV_1_LT_113"), new Text("Total\t" + record1));
            context.write(new Text("MTG_LTV_2_LT_120"), new Text("Total\t" + record1));
            context.write(new Text("MTG_LTV_3_GT_120"), new Text("Total\t" + record1));
            context.write(new Text("VEH_LTV_1_LT_66"), new Text("Total\t" + record1));
            context.write(new Text("VEH_LTV_2_LT_78"), new Text("Total\t" + record1));
            context.write(new Text("VEH_LTV_3_GT_78"), new Text("Total\t" + record1));
        }
    }


    //Credit Score Grouping
    public static class Score_Grouping_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {

            String record2 = value.toString();
            String[] tokens = record2.split(",");
            String Score_Bucket = "";
            if (!tokens[0].equals("ID")) {
                Double Score = Double.valueOf(tokens[6]);
                if (Score <= 0.33) {Score_Bucket = "BAD_OR_NO_SCORE";}
                else {Score_Bucket = "GOOD_SCORE";}
                String DF_Ind = tokens[1];
                context.write(new Text(Score_Bucket) , new Text("DF\t" + DF_Ind));
            }
        }
    }
    public static class Score_Total_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String record1 = value.toString();
            context.write(new Text("BAD_OR_NO_SCORE"), new Text("Total\t" + record1));
            context.write(new Text("GOOD_SCORE"), new Text("Total\t" + record1));
        }
    }

    //The "Master" reducer
    //Group by Portfolio - reducer works for all!
    public static class Grouping_Reducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int Total = 0;
            int Defaults = 0;
            int Total_Bad = 0;
            int Total_Full = 0;
            for (Text val : values){
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("DF")) {
                    Total++;
                    Defaults += Integer.parseInt(parts[1]);
                }
                else if (parts[0].equals("Total")) {
                    String[] Tot = val.toString().split("\t");
                    Total_Full += Integer.parseInt(Tot[2]);
                    Total_Bad += Integer.parseInt(Tot[3]);
                }
            }
            //The Default Rate Calculation
            Double ODR = Double.valueOf(Defaults)/Double.valueOf(Total);
            //The weights of evidence calculation
            Double WoE = Math.log((Double.valueOf(Total-Defaults)/Double.valueOf(Total_Full-Total_Bad))/(Double.valueOf(Defaults)/Double.valueOf(Total_Bad)));
            String Output_Str2 = String.format("%f\t%f", ODR, WoE);
            context.write(key, new Text(Output_Str2));
        }
    }


    //Combine WoEs in Final Dataset
    //The join table is loaded into memory. This can be done given there is not a lot of groups
    public static class Big_DS_Join_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] tokens = record.split(",");
            String ID = "";
            String Portfolio_Group = "";
            String LTV_Bucket = "";
            String IO_Bucket = "";
            String AGE_Bucket = "";
            String Score_Bucket = "";
            String DF_Ind = "";
            String PORTF_LTV_Bucket = "";
            if (!tokens[0].equals("ID")){
                Total_Chck++; //Count number of resulting rows for check

                ID = tokens[0];

                DF_Ind = tokens[1];

                //Portfolio Group, not too much to it
                Portfolio_Group = tokens[2].replace(" ","_");

                //LTV Group
                Double LTV = Double.valueOf(tokens[3]);
                if (LTV <= 0.8488) {LTV_Bucket = "LTV_1_LT_85";}
                else if (LTV <= 1){LTV_Bucket = "LTV_2_LT_100";}
                else {LTV_Bucket = "LTV_3_GT100";}

                //Installment group
                Double Instl = Double.valueOf(tokens[4]);
                if (Instl < 750) {IO_Bucket = "Instl_LT_750";}
                else {IO_Bucket = "Instl_GT_750";}

                //Age Group
                Double Age = Double.valueOf(tokens[4]);
                if (Age <= 35) {AGE_Bucket = "AGE_LT_35";}
                else if (Age <= 55) {AGE_Bucket = "AGE_LT_55";}
                else {AGE_Bucket = "AGE_GT_55";}

                //Score Group
                Double Score = Double.valueOf(tokens[4]);
                if (Score <= 0.33) {Score_Bucket = "BAD_OR_NO_SCORE";}
                else {Score_Bucket = "GOOD_SCORE";}

                //LTV and Portfolio Grouping
                if (Portfolio_Group.equals("Mortgage")){
                    if (LTV <= 1.132) {PORTF_LTV_Bucket = "MTG_LTV_1_LT_113";}
                    else if (LTV <= 1.198) {PORTF_LTV_Bucket = "MTG_LTV_2_LT_120";}
                    else {PORTF_LTV_Bucket = "MTG_LTV_3_GT_120";}
                }
                else {
                    if (LTV <= 0.6635) {PORTF_LTV_Bucket = "VEH_LTV_1_LT_66";}
                    else if (LTV <= 0.7841) {PORTF_LTV_Bucket = "VEH_LTV_2_LT_78";}
                    else {PORTF_LTV_Bucket = "VEH_LTV_3_GT_78";}
                }

                //The Output String
                String Output_Str = JT.get(Score_Bucket)
                        + "\t" + JT.get(PORTF_LTV_Bucket)
                        + "\t" + JT.get(IO_Bucket)
                        + "\t" + JT.get(AGE_Bucket)
                        + "\t" + DF_Ind;
                //System.out.println(Total_Chck); //Print out the number of rows
                context.write(new Text(ID), new Text(Output_Str));
            }
        }
    }


    public static class Small_DS_Join_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] record = value.toString().split("\t");
            JT.put(record[0] , record[2]);
        }
    }


    //The Hash table for storing the WoE Values
    public static Hashtable<String, String> JT = new Hashtable<String, String>();
    public static int Total_Chck = 0;
    public static void main(String[] args) throws Exception {

        //A sequence
        //Job 1 - Totals
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "All_Group_by");
        job1.setJarByClass(DIA_Project_Development.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/WoE_Groups/All_GB"));
        job1.setMapperClass(All_Mapper.class);
        job1.setReducerClass(All_Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        //Job 2 - Group by portfolio
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Group_By_Portfolio");
        job2.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job2, new Path(args[0]) , TextInputFormat.class , Portfolio_Grouping_Mapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1] + "/WoE_Groups/All_GB") , TextInputFormat.class , Portfolio_Total_Mapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/WoE_Groups/Portfolio_GB"));
        job2.setReducerClass(Grouping_Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);


        //Job 3 - Group by LTV
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Group_By_LTV");
        job3.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job3, new Path(args[0]) , TextInputFormat.class , LTV_Grouping_Mapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[1] + "/WoE_Groups/All_GB") , TextInputFormat.class , LTV_Total_Mapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/WoE_Groups/LTV_GB"));
        job3.setReducerClass(Grouping_Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);


        //Job 4 - Interest Only Indicator
        Configuration conf4 = new Configuration();
        Job job4 = new Job(conf4, "Group_By_IO");
        job4.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job4, new Path(args[0]) , TextInputFormat.class , IO_Grouping_Mapper.class);
        MultipleInputs.addInputPath(job4, new Path(args[1] + "/WoE_Groups/All_GB") , TextInputFormat.class , IO_Total_Mapper.class);
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/WoE_Groups/IO_GB"));
        job4.setReducerClass(Grouping_Reducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);


        //Job 5 - Age Groups
        Configuration conf5 = new Configuration();
        Job job5 = new Job(conf5, "Group_By_Age");
        job5.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job5, new Path(args[0]) , TextInputFormat.class , AGE_Grouping_Mapper.class);
        MultipleInputs.addInputPath(job5, new Path(args[1] + "/WoE_Groups/All_GB") , TextInputFormat.class , AGE_Total_Mapper.class);
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "/WoE_Groups/AGE_GB"));
        job5.setReducerClass(Grouping_Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);


        //Job 6 -  Groups
        Configuration conf6 = new Configuration();
        Job job6 = new Job(conf6, "Group_By_IO");
        job6.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job6, new Path(args[0]) , TextInputFormat.class , Score_Grouping_Mapper.class);
        MultipleInputs.addInputPath(job6, new Path(args[1] + "/WoE_Groups/All_GB") , TextInputFormat.class , Score_Total_Mapper.class);
        FileOutputFormat.setOutputPath(job6, new Path(args[1] + "/WoE_Groups/Score_GB"));
        job6.setReducerClass(Grouping_Reducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);


        //Job 7 -  Groups
        Configuration conf7 = new Configuration();
        Job job7 = new Job(conf7, "Group_By_PORTF_LTV");
        job7.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job7, new Path(args[0]) , TextInputFormat.class , PortfolioLTV_Grouping_Mapper.class);
        MultipleInputs.addInputPath(job7, new Path(args[1] + "/WoE_Groups/All_GB") , TextInputFormat.class , PortfolioLTV_Total_Mapper.class);
        FileOutputFormat.setOutputPath(job7, new Path(args[1] + "/WoE_Groups/PORTF_LTV_GB"));
        job7.setReducerClass(Grouping_Reducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);


        //Job8 - Joining it all together no need for reducer!
        Configuration conf8 = new Configuration();
        Job job8 = new Job(conf8, "Master_Join_Get_Vars1");
        job8.setJarByClass(DIA_Project_Development.class);
        MultipleInputs.addInputPath(job8, new Path(args[1] + "/WoE_Groups/AGE_GB") , TextInputFormat.class , Small_DS_Join_Mapper.class);
        MultipleInputs.addInputPath(job8, new Path(args[1] + "/WoE_Groups/IO_GB") , TextInputFormat.class , Small_DS_Join_Mapper.class);
        MultipleInputs.addInputPath(job8, new Path(args[1] + "/WoE_Groups/LTV_GB") , TextInputFormat.class , Small_DS_Join_Mapper.class);
        MultipleInputs.addInputPath(job8, new Path(args[1] + "/WoE_Groups/Portfolio_GB") , TextInputFormat.class , Small_DS_Join_Mapper.class);
        MultipleInputs.addInputPath(job8, new Path(args[1] + "/WoE_Groups/Score_GB") , TextInputFormat.class , Small_DS_Join_Mapper.class);
        MultipleInputs.addInputPath(job8, new Path(args[1] + "/WoE_Groups/PORTF_LTV_GB") , TextInputFormat.class , Small_DS_Join_Mapper.class);
        FileOutputFormat.setOutputPath(job8, new Path(args[1] + "/temp1"));


        //Job9 - Finishing off the join
        Configuration conf9 = new Configuration();
        Job job9 = new Job(conf9, "Master_Join_Get_Vars2");
        job9.setJarByClass(DIA_Project_Development.class);
        FileInputFormat.addInputPath(job9, new Path(args[0]));
        FileOutputFormat.setOutputPath(job9, new Path(args[1] + "/ABT"));
        job9.setMapperClass(Big_DS_Join_Mapper.class);
        job9.setOutputKeyClass(Text.class);
        job9.setOutputValueClass(Text.class);


        //Exit the job
        System.exit(job1.waitForCompletion(true)
                && job2.waitForCompletion(true)
                && job3.waitForCompletion(true)
                && job4.waitForCompletion(true)
                && job5.waitForCompletion(true)
                && job6.waitForCompletion(true)
                && job7.waitForCompletion(true)
                && job8.waitForCompletion(true)
                && job9.waitForCompletion(true)
                ? 0 : 1);
    }

}
