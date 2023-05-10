import com.google.gson.*;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class AcordarJenaExtractor {

    private File datasetsFolder;
    private FileWriter logFile;

    public static final HashSet<String> SUFFIXES= new HashSet<>(Arrays.asList("rdf", "rdfs", "ttl", "owl", "n3", "nt", "jsonld", "xml", "ntriples", "nq", "trig", "trix"));

    /** constructor
     * @param datasetsFolderPath path to the datasets folder
     * @param logFilePath path to the log file
     * @throws IOException if there are problems
     * @throws IOException if there are problems during the log file opening
     */
    public AcordarJenaExtractor(String datasetsFolderPath, String logFilePath) throws IOException {
        datasetsFolder = new File(datasetsFolderPath);
        if(!datasetsFolder.isDirectory())
            throw new IllegalArgumentException("The datasets folder path provided is not a directory path");
        if(!datasetsFolder.exists())
            throw new IllegalArgumentException("The datasets folder path provided does not exists");

        logFile = new FileWriter(logFilePath);

    }

    /**
     * @param fileName name of the file
     * @return true if the file has a valid RDF extension
     */
    private boolean isRDFFile(String fileName){
        return SUFFIXES.contains(fileName.split("\\.")[1]);
    }

    /**
     * This method update the dataset_metadata.json file after the dataset parsing
     * @param path to the dataset_metadata.json file
     * @param minedFiles number of mined files for the dataset by Jena
     */
    private void updateJSONFIle(String path, int minedFiles){
        JsonElement json = null;
        try {
            Reader reader = new FileReader(path, StandardCharsets.UTF_8);
            json = JsonParser.parseReader(reader);
            reader.close();
        } catch (Exception e) {
            System.out.println("Error while opening the dataset.json file: "+e);
        }

        //get the JsonObject to udpate
        JsonObject object = json.getAsJsonObject();

        object.addProperty("mined_files_jena", minedFiles);

        //write the json for updating the dataset_metadata.json file
        FileWriter file;
        try {
            file = new FileWriter(path, StandardCharsets.UTF_8);
            file.write(String.valueOf(object));
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * @param dataset file object of the dataset
     * @throws IOException if there are problem during the writing in the log file
     */
    public void mineDataset(File dataset) throws IOException {
        System.out.println("Mining dataset: "+dataset);

        HashMap<String, LinkedList<String>> data = new HashMap<>();
        data.put(StreamRDFParser.CustomTriple.CLASSES, new LinkedList<>());
        data.put(StreamRDFParser.CustomTriple.ENTITIES, new LinkedList<>());
        data.put(StreamRDFParser.CustomTriple.LITERALS, new LinkedList<>());
        data.put(StreamRDFParser.CustomTriple.PROPERTIES, new LinkedList<>());

        //list all the files
        File[] files = dataset.listFiles();

        int minedFiles = 0;

        for(File file: files){

            if(isRDFFile(file.getName())) {

                System.out.println("Processing: "+dataset.getName()+" File: "+file.getName());

                try {
                    StreamRDFParser parser = new StreamRDFParser(file.getPath());

                    while (parser.hasNext()) {
                        StreamRDFParser.CustomTriple triple = parser.next();

                        data.get(triple.getSubject().getKey()).add(triple.getSubject().getValue());
                        data.get(StreamRDFParser.CustomTriple.PROPERTIES).add(triple.getPredicate());
                        data.get(triple.getObject().getKey()).add(triple.getObject().getValue());

                    }

                    parser.close();
                    minedFiles++;

                } catch (Exception e) {
                    //remove from the exception message all the \n characters that can break the message
                    String errorMessage = e.getMessage().replace("\n", " ");
                    logFile.write("Dataset: " + dataset.getName() + "\nFile: " + file.getName() + "\nError: " + errorMessage + "\n");
                    logFile.flush();
                } catch (OutOfMemoryError e) {
                    logFile.write("Dataset: " + dataset.getName() + "\nFile: " + file.getName() + "\nError: JavaOutOfMemory\n");
                    logFile.flush();
                }
            }

        }

        //create the dataset_content_jena.json
        String datasetContentPath = dataset.getPath()+"/dataset_content_jena.json";
        FileWriter datasetContent = new FileWriter(datasetContentPath, false);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        gson.toJson(data, datasetContent);
        datasetContent.close();

        //update the dataset_metadata.json
        String datasetMetadataPath = dataset.getPath()+"/dataset_metadata.json";
        updateJSONFIle(datasetMetadataPath, minedFiles);
    }



    /**
     * This method mines all the datasets
     */
    public void mineDatasets() throws IOException {
        File[] datasets = datasetsFolder.listFiles();

        for(File dataset: datasets){

            if(dataset.isDirectory())
                mineDataset(dataset);
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        String datasetsFolder = "/home/manuel/Tesi/ACORDAR/Datasets";
        String logFilePath = "/home/manuel/Tesi/ACORDAR/Datasets/jena.txt";
        AcordarJenaExtractor e = new AcordarJenaExtractor(datasetsFolder, logFilePath);
        e.mineDatasets();

    }
}
