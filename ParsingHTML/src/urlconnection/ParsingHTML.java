/*
Desarrollar una aplicación que analice el contenido de la pagina  https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html, en la cual se incluyen varios
enlaces (links) para descargar archivos con formato CSV. El programa debe generar una lista con los nombres de los archivos CSV a los que se hace referencia
en csv.html, seguido del número de líneas/registros que contiene cada archivo CSV.

Para procesar cada archivo CSV, y obtener el número de líneas, se debe crear un hilo (thread) diferente.
 */

package urlconnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParsingHTML {
    public static final String TAG = ParsingHTML.class.getSimpleName();
    public static final Logger LOG = Logger.getLogger(TAG);

    public static void main(String[] args) {
        URL webPage = null;
        HttpURLConnection connection = null;

        try {
            webPage = new URL("https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html");
            connection = (HttpURLConnection) webPage.openConnection();

            System.out.println("\nArchivos CVS en Enlace Web:" + webPage + "\n");

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;

            ArrayList<String> csvArray = new ArrayList<>();

            while ((inputLine = in.readLine()) != null) {
                ArrayList<String> cvsRecuperado = recuperarCSV(inputLine);
                csvArray.addAll(cvsRecuperado);
            }

            //executor service para manejar threads
            ExecutorService executorService = Executors.newFixedThreadPool(csvArray.size());

            for (String csv : csvArray) {
                executorService.submit(new procesarCSV(webPage, csv));
            }

            executorService.shutdown();

            in.close();
        } catch (IOException ex) {
            LOG.severe(ex.getMessage());
        }
    }

    /*
    NOTA: Como la clase procesarCSV es la responsable de correr los CVS, cada uno de estos se procesa en un thread
    diferente para optimizar el programa, en especial si hay una alta cantidad de archivos y de lineas por leer.
     */
    private static class procesarCSV implements Runnable {
        private final URL webPage;
        private final String csvLink;

        procesarCSV(URL webPage, String csvLink) {
            this.webPage = webPage;
            this.csvLink = csvLink;
        }

        @Override
        public void run() {
            try {
                URL fullCSVURL = new URL(webPage, (new URL(webPage, csvLink)).toString());
                HttpURLConnection csvConnection = (HttpURLConnection) fullCSVURL.openConnection();
                int csvLineCount = contarLineasCSV(csvConnection);
                System.out.println("Archivo CSV: " + csvLink);
                System.out.println("Numero de lineas: " + csvLineCount);
                System.out.println("---------------------------------");
            } catch (IOException ex) {
                LOG.severe(ex.getMessage());
            }
        }
    }

    //contador de lineas por cada csv
    private static int contarLineasCSV(HttpURLConnection connection) throws IOException {
        BufferedReader csvReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        int contLinea = 0;

        while (csvReader.readLine() != null) {
            contLinea++;
        }
        csvReader.close();
        return contLinea;
    }

    //metodo para almacenar csv´s en un arraylist
    public static ArrayList<String> recuperarCSV(String input) {
        ArrayList<String> extractedStrings = new ArrayList<>();

        // expresion regular para delimitar el archivo csv
        Pattern pattern = Pattern.compile("<a href = \"(.*\\.csv)\">");
        Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            extractedStrings.add(matcher.group(1));
        }
        return extractedStrings;
    }
}
