package dataproc;

//imports required
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Loans {
    public static void main(String[] args) {
        Loans app = new Loans();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder().appName("my-java-dataproc").master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // Loading the CSV file from GCS Bucket
        Dataset<Row> dfLoans = spark.read().format("csv").option("header", "true").option("inferSchema", true)
                .load("gs://casestudy2bkt_1/ibrd-statement-of-loans-historical-data.csv");

        // Creates temporary view using DataFrame
        dfLoans.withColumnRenamed("Country", "country").withColumnRenamed("Country Code", "country_code")
                .withColumnRenamed("Disbursed Amount", "disbursed")
                .withColumnRenamed("Borrower's Obligation", "obligation")
                .withColumnRenamed("Interest Rate", "interest_rate").createOrReplaceTempView("loans");

        // Performs basic analysis of dataset
        Dataset<Row> dfDisbursement = spark.sql("SELECT country, country_code, "
                + "format_number(total_disbursement, 0) AS total_disbursement, "
                + "format_number(ABS(total_obligation), 0) AS total_obligation, "
                + "format_number(avg_interest_rate, 2) AS avg_interest_rate " + "FROM ( "
                + "SELECT country, country_code, " + "SUM(disbursed) AS total_disbursement, "
                + "SUM(obligation) AS total_obligation, " + "AVG(interest_rate) AS avg_interest_rate " + "FROM loans "
                + "GROUP BY country, country_code " + "ORDER BY total_disbursement DESC " + "LIMIT 25)");

        // Saves results to single CSV file in GCS Bucket
        dfDisbursement.repartition(1).write().mode(SaveMode.Overwrite).format("csv").option("header", "true")
                .save("gs://casestudy2bkt_1/ibrd-loan-summary");

        System.out.println("Results successfully written to CSV file");
    }
}