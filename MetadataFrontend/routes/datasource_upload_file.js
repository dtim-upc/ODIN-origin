var formidable = require('formidable'),
    fs = require('fs'),
    request = require('request'),
    config = require(__dirname + '/../config'),
    upload_path = config.FILES_PATH;

exports.uploadFile = function (req, res) {

    var form = new formidable.IncomingForm({uploadDir: upload_path});
    var resultFile = new Object();
    form.on('file', function(name, file) {
            var newFilePath = form.uploadDir + "/"+Date.now() + '-' + file.name
            fs.rename(file.path, newFilePath,
                function (err) {if (err) throw err;});
            resultFile.path = newFilePath;
    });

    form.parse(req, function (err, fields, files) {
        // res.status(200).json(resultFile);
        const formData = {
            // Pass data via Streams
            "file":  fs.createReadStream(resultFile.path)
        };
        request.post({url:config.METADATA_DATA_LAYER_URL + "dataSource/upload", formData: formData}, function optionalCallback(err, httpResponse, body) {
            if (err) {
                return console.error('upload failed:', err);
            }
            fs.unlink(resultFile.path, (err) => {
                if (err) {
                    console.log("failed to delete local file from fronent:"+err);
                }
            });

            resultFile.path = body; //update path using the one provided by metadatastoraqe
            console.log("Datasource upload successful, file path is: "+resultFile.path);
            res.status(200).json(resultFile);
        });

    });
};
