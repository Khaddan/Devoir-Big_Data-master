package org.aboufariss;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("VOLS")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> volsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_aer")
                .option("dbtable", "vols")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> reservationsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_aer")
                .option("dbtable", "reservations")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> passagersDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_aer")
                .option("dbtable", "passangers")
                .option("user", "root")
                .option("password", "")
                .load();

        volsDF.createOrReplaceTempView("vols");
        reservationsDF.createOrReplaceTempView("reservations");
        passagersDF.createOrReplaceTempView("passagers");

        // Afficher pour charque vol, le nombre de passagers
        Dataset<Row> result = spark.sql(
                "SELECT v.Id AS ID_VOL, v.Date_Depart, COUNT(DISTINCT r.Id_passanger) AS NOMBRE " +
                        "FROM vols v, " +
                        "reservations r WHERE v.Id = r.Id_Vol " +
                        "GROUP BY v.Id, v.Date_Depart"
        );

        // Afficher la liste des vols en cours
        Dataset<Row> currentVols = spark.sql(
                "SELECT * FROM vols WHERE Date_Depart=CURRENT_DATE; "
        );

        result.show((int) result.count());
        currentVols.show();
        spark.stop();
    }
}