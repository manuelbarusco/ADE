package deduplication;

import com.google.gson.*;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.AsyncParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AcordarJenaExtractorDeduplicationLabels {

    private File datasetsFolder;                                // folder where there are all the datasets
    private String logFilePath;                                 // path to the error log file
    private FileWriter logFile;                                 // text file where to log all the errors
    private static final int LIMIT_FILE_SIZE = 500;             // Limit size to a file that must be parsed

    public static final HashSet<String> SUFFIXES= new HashSet<>(Arrays.asList("rdf", "rdfs", "ttl", "owl", "n3", "nt", "jsonld", "xml", "ntriples", "nq", "trig", "trix"));

    public static final String CLASSES = "classes";
    public static final String PROPERTIES = "properties";
    public static final String LITERALS = "literals";
    public static final String ENTITIES = "entities";

    /** constructor
     * @param datasetsFolderPath path to the datasets folder
     * @param logFilePath path to the log file
     */
    public AcordarJenaExtractorDeduplicationLabels(String datasetsFolderPath, String logFilePath) {
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
     * @param minedFiles list of mined files for the dataset by Jena
     */
    private void updateJSONFIle(String path, List<String> minedFiles){
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

        JsonArray minedFilesJSON = new JsonArray();
        for(String file: minedFiles)
            minedFilesJSON.add(file);

        datasetMetadata.addProperty("mined_jena", true);
        datasetMetadata.add("mined_files_jena", minedFilesJSON);

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

        //lists for information extraction
        HashMap<String, LinkedList<String>> data = new HashMap<>();
        data.put(CLASSES, new LinkedList<>());
        data.put(ENTITIES, new LinkedList<>());
        data.put(LITERALS, new LinkedList<>());
        data.put(PROPERTIES, new LinkedList<>());

        //list all the files
        File[] files = dataset.listFiles();
        List<String> minedFiles = new LinkedList<String>();

        //list of all the triples for the deduplication
        HashSet<Triple> triples = new HashSet<>();

        //Hashmap for URI -> label mapping
        HashMap<String, String> uriLabelMap = new HashMap<>();

        // ------------------- TRIPLE DEDUPLICATION ------------------------ //

        for(File file: files){
            double fileSize = (double) file.length() / (1024*1024);

            //check if the file is greater than the limit file size in MB
            if (fileSize > LIMIT_FILE_SIZE) {
                logFile.write("Dataset: " + dataset.getName() + "\nFile: " + file.getName() + "\nError: bigger than " + LIMIT_FILE_SIZE + " MB\n");
                logFile.flush();
            }

            if(isRDFFile(file.getName()) && fileSize < LIMIT_FILE_SIZE) {
                //System.out.println(file.getName());

                try {
                    IteratorCloseable<Triple> iterator = AsyncParser.asyncParseTriples(file.getPath());

                    while (iterator.hasNext()) {
                        Triple triple = iterator.next();
                        triples.add(triple);

                        String predicate = triple.getPredicate().getLocalName();

                        if(predicate.contains("label")){
                            String label = triple.getObject().toString(false);
                            if (label.contains("^"))
                                label = label.split("\\^")[0];
                            else if (label.contains("@"))
                                label = label.split("@")[0];
                            uriLabelMap.put(triple.getSubject().toString(), label);
                        }
                    }

                    iterator.close();
                    minedFiles.add(file.getName());

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

        // --------------------------------------------------------

        //read the deduplicated triples and assign subject, predicate and object to the lists
        Iterator<Triple> iterator = triples.iterator();
        while (iterator.hasNext()) {
            Triple triple = iterator.next();
            try {
                String predicate = triple.getPredicate().getLocalName();

                data.get(PROPERTIES).add(predicate);

                String subjectValue = uriLabelMap.getOrDefault(triple.getSubject().toString(), triple.getSubject().toString());
                String objectValue = uriLabelMap.getOrDefault(triple.getObject().toString(), triple.getObject().toString());

                if (triple.getSubject().isURI())
                    data.get(ENTITIES).add(subjectValue);

                if (triple.getObject().isURI()) {
                    if (predicate.equals("type"))
                        data.get(CLASSES).add(objectValue);
                } else if (triple.getObject().isLiteral()) {
                    String value = triple.getObject().toString(false);
                    if (value.contains("^"))
                        value = value.split("\\^")[0];
                    else if (value.contains("@"))
                        value = value.split("@")[0];
                    data.get(LITERALS).add(value);
                }

                iterator.remove();
            } catch (Exception e) {
                System.out.println(e);
                System.out.println(dataset.getName());
            }
        }


        //create the dataset_content_jena.json or dataset_content_jenaURI.json
        String datasetContentPath = dataset.getPath()+"/dataset_content_jena_deduplication.json";
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
        String datasetsFolder = "";

        //itialize the datasets folder path
        if(args.length > 0)
            datasetsFolder = args[0];
        else
            datasetsFolder =  "/media/manuel/Tesi/Datasets";

        String logFilePath = "src/main/java/deduplication/logs/jena_miner_error_log_deduplication.log";

        boolean parseUri = true;
        AcordarJenaExtractorDeduplicationLabels e = new AcordarJenaExtractorDeduplicationLabels(datasetsFolder, logFilePath);

        boolean resume = false;
        e.mineDatasets(resume);

    }
}
