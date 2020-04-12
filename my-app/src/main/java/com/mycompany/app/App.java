package com.mycompany.app;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;

import java.awt.*;
import java.awt.event.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;

import java.io.*;
import java.util.*;

public class App {
    static JFrame frame = new JFrame("kjt29 Search Engine");
    static JTextArea fileList = new JTextArea(100, 100);
    //static JTextArea searchBox = new JTextArea(100, 100);
    //private static String searchTerm;
    private static ActionListener listener = new Listener();
    static ArrayList<String> pathList = new ArrayList<>();
    static ArrayList<String> nameList = new ArrayList<>();
    static HashMap<String, Integer> results = new HashMap<>();
    private static String finalOutput;
    private static String projectId = "cloudproject-273403";
    private static String region = "us-central1";
    
    public static void main(String[] args) {

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 800);
        frame.setLayout(new GridBagLayout());
        frame.setVisible(true);

        Box box = Box.createVerticalBox();
        box.add(addButton("Choose Files", listener));
        box.add(Box.createVerticalStrut(15));
        fileList.setEditable(false);
        box.add(fileList);
        box.add(Box.createVerticalStrut(15));
        // searchBox.append("Enter search term");
        // box.add(searchBox);
        box.add(Box.createVerticalStrut(15));
        box.add(addButton("Construct Inverted Indicies", listener));
        
        frame.add(box);
        frame.repaint();

    }
    
    private static class Listener implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {

            if (e.getActionCommand().equals("Choose Files")) {
                // Make a file chooser to get the files for the search
                JFileChooser chooser = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                chooser.setMultiSelectionEnabled(true);
                chooser.showOpenDialog(frame);
                File[] filesArray = chooser.getSelectedFiles();
                for (int i=0; i<filesArray.length; i++) {
                    fileList.append(filesArray[i].getName() + ", ");
                    pathList.add(filesArray[i].getAbsolutePath());
                    nameList.add(filesArray[i].getName());
                }
            }

            else if (e.getActionCommand().equals("Construct Inverted Indicies")) {
                //searchTerm = searchBox.getText();
                // Send the files and return the results
                for (int i = 0; i < pathList.size(); i++) {
                    // Upload the files
                    try {
                        System.out.println("Uploading: " + pathList.get(i));
                        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
                        
                        BlobId blobId = BlobId.of("dataproc-staging-us-central1-1093852109547-d5pnwsmu", "data/" + nameList.get(i));
                        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                        storage.create(blobInfo, Files.readAllBytes(Paths.get(pathList.get(i))));
                    } catch (IOException ex) {
                        ex.printStackTrace(); 
                    }
                }

                // Submit the job to the GCP cluster
                try {
                    System.out.println("Submitting Hadoop MapReduce Job");
                    submitHadoopJob();
                } catch (IOException exception) {
                    exception.printStackTrace();
                }

                // Get all the files from output and display the search results
                // Need to wait until job is done
                while (true) {
                    try {
                        System.out.println("Trying to download results...");
                        ArrayList<byte[]> merge = new ArrayList<byte[]>();
                        int arrayLength = 0;

                        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
                        System.out.println("Connected to storage bucket");
                        Page<Blob> blobs = storage.list("dataproc-staging-us-central1-1093852109547-d5pnwsmu", BlobListOption.prefix("output/"));

                        Iterator<Blob> iterator = blobs.iterateAll().iterator();
                        iterator.next();    // Skip the first file (success file)

                        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

                        while (iterator.hasNext()) {
                            Blob blob = iterator.next();
                            System.out.println("Got blob: " + blob.getName());

                            // If a file in progress is found, back out and wait 10 sec
                            if (blob.getName().contains("temp"))
                                throw new IOException();
                            blob.downloadTo(byteStream);
                            merge.add(byteStream.toByteArray());
                            arrayLength += byteStream.size();
                            byteStream.reset();
                        }
                        byteStream.close();

                        // Merge the data into one file
                        byte[] byteArray = new byte[arrayLength];
                        
                        int destination = 0;
                        for (byte[] data: merge) {
                            System.arraycopy(data, 0, byteArray, destination, data.length);
                            destination += data.length;
                        }

                        finalOutput = new String(byteArray);
                        
                        break;
                    } catch(Exception notDone) {
                        //notDone.printStackTrace();
                        try {
                            // Job isn't done, so lets wait a bit
                            System.out.println("Bucket not ready. Sleeping for 10 seconds...");
                            Thread.sleep(10000);
                        } catch (InterruptedException exce) {
                            exce.printStackTrace();
                        }
                    }
                }

                // So I tried to go line by line through all of the results to get a specific word
                // but I think the file became too long. 

                // try {
                //     Scanner sc = new Scanner(finalOutput); 
                //     String line;
                //     while ((line = sc.nextLine()) != null) {
                //         if (line.contains(searchTerm)) {
                //             results.put(line, 1);
                //         }
                //     }
                //     sc.close();
                // } catch(Exception except) {
                //     except.printStackTrace();
                // }
                

                // Display the results
                frame.getContentPane().removeAll();
                JLabel label = new JLabel("Word   Document   Count");
                label.setHorizontalAlignment(SwingConstants.LEFT);
                Box box = Box.createVerticalBox();
                box.add(label);
                
                //System.out.print("Final output: " + finalOutput);
                fileList = new JTextArea(600, 500);
                
                // Iterator hmIterator = results.entrySet().iterator(); 
                // while (hmIterator.hasNext()) { 
                //     Map.Entry<String, Integer> mapElement = (Map.Entry)hmIterator.next(); 
                //     fileList.append(mapElement.getKey().toString());
                // } 
                //fileList.append(results.toString());

                fileList.append(finalOutput);
                fileList.setCaretPosition(0);
                //System.out.println(fileList.getText());
                JScrollPane scrollPane = new JScrollPane(fileList);
                scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
                scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
                scrollPane.setPreferredSize(new Dimension(400, 500));
                scrollPane.setMinimumSize(new Dimension(400, 500));
                box.add(scrollPane);
                
                frame.add(box);
                frame.getContentPane().validate();
                frame.repaint();
            }
        }
    }

    private static JButton addButton(String name, ActionListener l) {
        JButton button = new JButton(name);
        button.setAlignmentY(Component.CENTER_ALIGNMENT);
        button.addActionListener(l);
        frame.add(button);
        return button;
    }

    public static void submitHadoopJob() throws IOException {
        Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), 
        new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault().createScoped(DataprocScopes.all())))
        .setApplicationName("InvertedIndexSearchEngine").build();
                    
        dataproc.projects().regions().jobs().submit(
            projectId, region, new SubmitJobRequest()
                .setJob(new Job()
                    .setPlacement(new JobPlacement()
                        .setClusterName("my-cluster"))
                    .setHadoopJob(new HadoopJob()
                        .setMainClass("WordCount")
                        .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/JAR/WordCount.jar"))
                        .setArgs(ImmutableList.of(
                            "gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/data", "gs://dataproc-staging-us-central1-1093852109547-d5pnwsmu/output")))))
            .execute();
    }
}