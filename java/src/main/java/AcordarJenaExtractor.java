import com.google.gson.*;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class AcordarJenaExtractor {

    private File datasetsFolder;                                // folder where there are all the datasets
    private String logFilePath;                                 // path to the error log file

    private FileWriter logFile;                                 // text file where to log all the errors

    private static final int LIMIT_FILE_SIZE = 500;             // Limit size to a file that must be parse

    public static final HashSet<String> SUFFIXES= new HashSet<>(Arrays.asList("rdf", "rdfs", "ttl", "owl", "n3", "nt", "jsonld", "xml", "ntriples", "nq", "trig", "trix"));

    /** constructor
     * @param datasetsFolderPath path to the datasets folder
     * @param logFilePath path to the log file
     */
    public AcordarJenaExtractor(String datasetsFolderPath, String logFilePath) {
        datasetsFolder = new File(datasetsFolderPath);
        if(!datasetsFolder.isDirectory())
            throw new IllegalArgumentException("The datasets folder path provided is not a directory path");
        if(!datasetsFolder.exists())
            throw new IllegalArgumentException("The datasets folder path provided does not exists");

        File file = new File(logFilePath);
        if(file.isDirectory())
            throw new IllegalArgumentException("The error log file path provided points to a directory");

        this.logFilePath = logFilePath;

    }

    /**
     * @param fileName name of the file
     * @return true if the file has a valid RDF extension
     */
    private boolean isRDFFile(String fileName){
        if(fileName.contains("."))
            return SUFFIXES.contains(fileName.split("\\.")[1]);
        return false;
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
        } catch (IOException e) {
            System.out.println("Error while reading the dataset_metadata.json file: "+e);
        }

        //get the JsonObject to udpate
        JsonObject datasetMetadata = json.getAsJsonObject();

        datasetMetadata.addProperty("mined_files_jena", minedFiles);
        datasetMetadata.addProperty("mined_jena", true);

        try{
            FileWriter datasetMetadataFile=new FileWriter(path, StandardCharsets.UTF_8);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(datasetMetadata, datasetMetadataFile);
            datasetMetadataFile.close();
        } catch (IOException e){
            System.out.println("Error while updating the dataset_metadata.json file: "+e);
        }
    }


    /**
     * @param dataset file object of the dataset
     * @throws IOException if there are problem during the writing in the log file
     */
    public void mineDataset(File dataset) throws IOException {
        //System.out.println("Mining dataset: "+dataset);

        HashMap<String, LinkedList<String>> data = new HashMap<>();
        data.put(StreamRDFParser.CustomTriple.CLASSES, new LinkedList<>());
        data.put(StreamRDFParser.CustomTriple.ENTITIES, new LinkedList<>());
        data.put(StreamRDFParser.CustomTriple.LITERALS, new LinkedList<>());
        data.put(StreamRDFParser.CustomTriple.PROPERTIES, new LinkedList<>());

        //list all the files
        File[] files = dataset.listFiles();

        int minedFiles = 0;

        for(File file: files){

            double fileSize = file.length() / (1024*1024);

            //check if the file is greater than the limit file size in MB
            if (fileSize > LIMIT_FILE_SIZE) {
                logFile.write("Dataset: " + dataset.getName() + "\nFile: " + file.getName() + "\nError: bigger than " + LIMIT_FILE_SIZE + " MB\n");
                logFile.flush();
            }

            if(isRDFFile(file.getName()) && fileSize < LIMIT_FILE_SIZE) {
                //System.out.println(file.getName());

                try {
                    StreamRDFParser parser = new StreamRDFParser(file.getPath());

                    while (parser.hasNext()) {
                        StreamRDFParser.CustomTriple triple = parser.next();

                        String subjectValue = triple.getSubject().getValue();
                        String objectValue = triple.getObject().getValue();

                        data.get(StreamRDFParser.CustomTriple.PROPERTIES).add(triple.getPredicate());

                        //check for non-empty values in subject or object values
                        if(!subjectValue.isBlank())
                            data.get(triple.getSubject().getKey()).add(subjectValue);

                        if(!objectValue.isBlank())
                            data.get(triple.getObject().getKey()).add(objectValue);

                    }

                    parser.close();
                    minedFiles++;

                } catch (Exception e) {
                    //remove from the exception message all the \n characters that can break the message
                    String errorMessage = e.getMessage();
                    if(errorMessage!=null)
                        errorMessage = errorMessage.replace("\n", " ");
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
        FileWriter datasetContent = new FileWriter(datasetContentPath, StandardCharsets.UTF_8);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        gson.toJson(data, datasetContent);
        datasetContent.close();

        //update the dataset_metadata.json
        String datasetMetadataPath = dataset.getPath()+"/dataset_metadata.json";
        updateJSONFIle(datasetMetadataPath, minedFiles);
    }

    /**
     * @param dataset File object relative to a dataset directory
     * @return True if the dataset was already mined by Jena else False
     */
    private boolean isMined(File dataset){

        //check the dataset_metadata.json file
        String path = dataset.getPath()+"/dataset_metadata.json";

        JsonElement json = null;
        try {
            Reader reader = new FileReader(path, StandardCharsets.UTF_8);
            json = JsonParser.parseReader(reader);
            reader.close();
        } catch (IOException e) {
            System.out.println("Error while reading the dataset_metadata.json file: "+e);
        }

        //get the JsonObject to udpate
        JsonObject datasetMetadata = json.getAsJsonObject();

        if(datasetMetadata.has("mined_jena"))
            return datasetMetadata.get("mined_jena").getAsBoolean();
        else
            return false;

    }

    /**
     * This method mines all the datasets
     *
     * @param resume boolean that indicates if the mine process has to be resumed or started from the beginning
     * @throws IOException if there are problems when opening the error log file
     */
    public void mineDatasets(boolean resume) throws IOException {

        if(resume)
            logFile = new FileWriter(logFilePath, true);
        else
            logFile = new FileWriter(logFilePath);

        File[] datasets = datasetsFolder.listFiles();

        int nDatasets = 0;

        for(File dataset: datasets){

            boolean mined = false;

            //if resume true check if the dataset was already mined, else mine the dataset even if was already mined
            if (resume)
                mined = isMined(dataset);

            if(dataset.isDirectory() && !mined) {
                mineDataset(dataset);
                nDatasets++;
                if(nDatasets % 1000 == 0)
                    System.out.println("Mined: "+nDatasets+" datasets");
            }

        }

        logFile.close();
    }


    public static void main(String[] args) throws IOException {
        String datasetsFolder = "/media/manuel/Tesi/Datasets";
        //String datasetsFolder = "/home/manuel/Tesi/ACORDAR/Datasets";
        String logFilePath = "/home/manuel/Tesi/Codebase/ADE/logs/jena_miner_error_log.txt";
        AcordarJenaExtractor e = new AcordarJenaExtractor(datasetsFolder, logFilePath);

        boolean resume = false;
        e.mineDatasets(resume);

    }
}
