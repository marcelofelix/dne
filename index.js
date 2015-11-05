'use strict'

var mongoose = require('mongoose');
var _ = require('lodash');
var fs = require('fs');
var rl = require('readline');
var iconv = require('iconv-lite');
var glob = require('glob');

mongoose.connect('docker/locations');
var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error'));

var file = '/Users/marcelofelix/Downloads/eDNE_Master_1509/Delimitado/LOG_LOGRADOURO_AC.TXT';

var base = {
    correiosId: Number,
    parentLocationId: Number,
    operation: String
}

var Location = mongoose.model('locations', _.assign(base, {
    uf: String,
    name: String,
    zip: String,
    status: String,
    type: String,
    abbreviation: String,
    cityId: String,
    oldZip: String
}));

var Street = mongoose.model('street', _.assign(base, {
    startNeighborhoodId: Number,
    finalNeighborhoodId: Number,
    name: String,
    complement: String,
    zip: String,
    type: String,
    typeIsAPrefixOfTheName: Boolean,
    abbreviation: String,
    oldZipCode: String
}));

glob('/Users/marcelofelix/Downloads/eDNE_Master_1509/Delimitado/**/*.TXT',
    function(err, files) {
        files.forEach(function(f) {
            if (f.indexOf('LOG_LOGRADOURO') > -1) {
                console.log(f);
                rl.createInterface({
                        input: fs.createReadStream(f)
                            .pipe(iconv.decodeStream('ISO-8859-1'))
                    })
                    .on('line', function(data) {
                        var value = data.split('@');
                        new Street({
                            correiosId: value[0],
                            parentLocationId: value[2],
                            startNeighborhoodId: value[3],
                            finalNeighborhoodId: value[4],
                            name: value[5],
                            complement: value[6],
                            zip: value[7],
                            type: value[8],
                            typeIsAPrefixOfTheName: value[9],
                            abbreviation: value[10],
                            operation: value[11],
                            oldZipCode: value[12]
                        }).save(function(error) {
                            console.log('Saving');
                            if (error)
                                console.log(error);
                        });
                    })
                    .on('end', function() {
                        console.log('Fim');
                    });
            };
        })
    });
