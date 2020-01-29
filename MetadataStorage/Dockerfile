FROM openjdk:8
VOLUME /usr/src/app/MetadataStorage/Output/
WORKDIR /usr/src/app/MetadataStorage
COPY /build/libs/ODIN-1.0.jar .
COPY ./config.dockerUsr.properties .
COPY /src/main/resources/ /usr/src/app/MetadataStorage/resources/
COPY /lib/sqlite4java-392 /usr/src/app/MetadataStorage/lib/sqlite4java-392
RUN mkdir /usr/src/app/MetadataStorage/Uploads
# RUN mkdir /usr/src/app/MetadataStorage/Output
EXPOSE 8082
CMD [ "java", "-Xmx2g","-jar","ODIN-1.0.jar","/usr/src/app/MetadataStorage/config.dockerUsr.properties"]
