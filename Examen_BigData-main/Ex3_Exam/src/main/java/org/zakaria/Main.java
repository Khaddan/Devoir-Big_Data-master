package org.aboufariss;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Configuration Spark
        SparkSession spark = SparkSession.builder()
                .appName("PlanesWithMostIncidents")
                .master("local[*]")
                .getOrCreate();

        // Lecture des données CSV des incidents
        Dataset<Row> incidentsDF = spark.read()
                .format("csv")
                .option("header", true)
                .load("incidents.csv");

        // Création d'une vue temporaire pour effectuer des requêtes SQL
        incidentsDF.createOrReplaceTempView("incidents");

        // Requête SQL pour obtenir l'avion ayant le plus d'incidents
        String query = "SELECT no_avion, COUNT(*) AS incident_count " +
                "FROM incidents " +
                "GROUP BY no_avion " +
                "ORDER BY incident_count DESC " +
                "LIMIT 1";

        // Exécution de la requête SQL
        Dataset<Row> result = spark.sql(query);

        // Affichage des résultats
        result.show();

        // Fermeture du contexte Spark
        spark.close();

    }
}