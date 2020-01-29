FROM node:12.6
# App directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY /MetadataFrontend/package*.json ./
RUN npm install
COPY /MetadataFrontend/. .

EXPOSE 3000

CMD [ "npm", "start" ]
