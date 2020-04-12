# Mini Search Engine
An inverted index implementation of a search engine using Hadoop MapReduce, Maven, and an ugly Java GUI.
Allows a user to upload text files and receive a wordcount output of all the documents. 

# Requirements:
1. First Java Application Implementation and Execution on Docker
2. Docker to Local (or GCP) Cluster Communication
3. Inverted Indexing MapReduce Implementation and Execution on the Cluster (GCP)

# Video walkthrough link:
https://youtu.be/G5r0hy5nz4U

# To build:
```bash
docker build -t cloudproject .
```

# To run (path to credentials must be within the project, display variable must be set for GUI to work. May be system-dependent):
```bash
docker run -it --privileged -e DISPLAY=10.0.0.136:0 -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json cloudproject mvn package exec:java -D exec.mainClass=com.mycompany.app.App
```

Getting X11 to play well with Docker was a bit difficult. I had to install socat and Xquartz, following along this tutorial: https://cntnr.io/running-guis-with-docker-on-mac-os-x-a14df6a76efc