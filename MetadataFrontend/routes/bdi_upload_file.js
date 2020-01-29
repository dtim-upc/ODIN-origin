/**
 * Created by Kashif Rabbani
 */
var formidable = require('formidable'),
    fs = require('fs'),
    request = require('request'),
    config = require(__dirname + '/../config'),
    upload_path = config.FILES_PATH;

exports.uploadFile = function (req, res) {

    var uploadedFile = [],
        form = new formidable.IncomingForm(),
        givenFileName = '',
        fileType = '',
        sql_JDBC = '';
    form.uploadDir = config.FILES_PATH;

    // Invoked when a file has finished uploading.
    form.on('file', function (name, file) {
        console.log("Inside form.on function");
        var filename = '';
        if (file.type === 'text/xml' || file.type === 'application/json' || file.type === 'application/vnd.ms-excel' || file.type == 'text/csv' ) {
            // Assign new file name
            filename = Date.now() + '-' + file.name;

            // Move the file with the new file name
            fs.rename(file.path, upload_path + "/" + filename,function (err) {
                if (err) throw err;
                console.log('renamed complete');
            });

            // Add to the list of uploads
            uploadedFile.push({
                status: true,
                filename: filename,
                type: fileType,
                givenName: givenFileName,
                filePath: upload_path + '/' + filename
            });
        } else {
            uploadedFile.push({
                status: false,
                filename: file.name,
                message: 'INVALID'
            });
            fs.unlink(file.path);
        }
    });

    form.on('error', function (err) {
        console.log('Error occurred during processing - ' + err);
    });

    // Invoked when all the fields have been processed.
    form.on('end', function () {
        console.log('All the request fields have been processed.');
    });

    // Parse the incoming form fields.
    form.parse(req, function (err, fields, files) {
        var uploadedFileProcessed = [];
        uploadedFile.forEach(function (obj) {
            console.log(obj);
            if (obj.hasOwnProperty('type')) {
                console.log("***")
                console.log(obj.type);
                if(obj.type =="csv" || obj.type =="json" || obj.type == "xml"){

                    const formData = { "file":  fs.createReadStream(obj.filePath)};
                    request.post({url:config.METADATA_DATA_LAYER_URL + "dataSource/upload", formData: formData}, function optionalCallback(err, httpResponse, body) {
                        if (err) {
                            console.log("error uploading file: "+err)
                            return console.error('upload failed:', err);
                        }
                        fs.unlink(obj.filePath, (err) => {
                            if (err) {
                                console.log("failed to delete local file from metadataFronend:"+err);
                            }
                        });
                        obj.filePath = body; //update path using the one provided by metadatastoraqe
                        uploadedFileProcessed.push(obj);
                        console.log("Datasource upload successful, body is: "+body);
                        console.log("Datasource upload successful, file path is: "+obj.filePath);
                        res.status(200).json(uploadedFileProcessed);
                    });
                }else{
                    uploadedFileProcessed.push(obj);
                    res.status(200).json(uploadedFileProcessed);
                }
            } else {
                uploadedFileProcessed.push(obj);
                res.status(200).json(uploadedFileProcessed);
            }

        });

    });

    form.on('field', function (name, value) {
        if (name === 'givenName') {
            givenFileName = value;
        }
        if (name === 'givenType') {
            fileType = value;
        }
        if (name === 'sql_jdbc') {
            sql_JDBC = value;
            uploadedFile.push({
                status: true,
                givenName: givenFileName,
                type: fileType,
                filePath: sql_JDBC
            });
        }
    });
};
