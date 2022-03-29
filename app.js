'use strict';
const axios = require('axios');
const fs = require('fs');
const StreamArray = require('stream-json/streamers/StreamArray');
const { Writable } = require('stream');
const fileStream = fs.createReadStream('data.json');
const jsonStream = StreamArray.withParser();
const processingStream = new Writable({
    write({
        key,
        value
    }, encoding, callback) {

        //some async operations
        setTimeout(() => {
            getPurchasedProducts(value)

            //console.log(key, value);
            //Runs one at a time, need to use a callback for that part to work
            callback();
        }, 500);
    },
    //Don't skip this, as we need to operate with objects, not buffers
    objectMode: true
});
//Pipe the streams as follows
fileStream.pipe(jsonStream.input);
jsonStream.pipe(processingStream);
//So we're waiting for the 'finish' event when everything is done.
processingStream.on('finish', () => console.log('All done'));

function append(file, data) {
    fs.appendFile(file, JSON.stringify(data) + "\n", function (err) {
        if (err) {
            console.log("Append is failed.")
        } else {
            // done
        }
    })
}

function getPurchasedProducts(purchase) {
    var data = JSON.stringify({
        "user_id": purchase["user_id"],
    });

    var config = {
        method: 'post',
        url: 'https://purchase_products_endpoint_',
        headers: {
            'Content-Type': 'application/json'
        },
        data: data
    };

    axios(config)
        .then(function (response) {            
            record["purchase_products"] =response.data["purchase_products"]
            record["last_purchase"] =response.data["last_purchase"]

            append('output.csv', Object.values(record))
        })
        .catch(function (error) {
            append('error.txt', error)
        });
}