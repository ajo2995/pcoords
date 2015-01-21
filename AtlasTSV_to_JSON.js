#!/usr/bin/env node
var fs = require('fs');
var filename = process.argv[2];

var columns = [];
var data = {};
var n=0;
require('readline').createInterface({
  input: fs.createReadStream(filename),
  terminal: false
}).on('line', function(line) {
  if (line.match(/^Gene\sID/)) {
    columns = line.split('\t');
    for(var i=2;i<columns.length;i++) {
      data[columns[i]] = {v:[],nulls:[]};
    }
  }
  else if (!line.match(/^#/)) {
    var row = line.split('\t');
    for(var i=2;i<columns.length;i++) {
      if (row[i] === '') {
        data[columns[i]].nulls.push(n);
        data[columns[i]].v.push(0);
      }
      else {
        data[columns[i]].v.push(+row[i]);
      }
    }
    n++;
  }
}).on('close', function() {
  // dump the json
  console.log(JSON.stringify(data));
});
