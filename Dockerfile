# Get the java and Maven image
FROM maven:3.6.3-openjdk-15

RUN yum -y install libX11-devel.x86_64
RUN yum -y install libXext.x86_64
RUN yum -y install libXrender.x86_64
RUN yum -y install libXtst.x86_64
# Include env variables for Google credentials here or do it on execution
COPY . /project
WORKDIR /project/my-app