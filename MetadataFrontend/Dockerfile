FROM node:12.6
WORKDIR /usr/src/app/MetadataFrontend
COPY package*.json /usr/src/app/MetadataFrontend/
RUN npm install
RUN mkdir /usr/src/app/MetadataFrontend/Uploads
COPY . .
COPY config.docker.js /usr/src/app/MetadataFrontend/config.js
EXPOSE 3000
CMD [ "npm", "start" ]
