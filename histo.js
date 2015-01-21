#!/usr/bin/env node

// read and parse JSON
var time = process.hrtime();
require('fs').readFile(process.argv[2], function (err, json) {
  if (err) throw err;
  var data = JSON.parse(json);
  var colNames = Object.keys(data);
  var diff = process.hrtime(time);
  var ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('loaded ',colNames.length,'x',data[colNames[0]].v.length,'in',ms,'ms');

  // compute distribution on each column
  time = process.hrtime();
  preprocess(data,50);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('preprocessed in',ms,'ms');

  // calculate a 2D distribution between a pair of columns
  time = process.hrtime();
  var x = dist2D(data[colNames[2]],data[colNames[3]]);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('dist2D in',ms,'ms');

  // range query on a column
  time = process.hrtime();
  var hits = filter(data[colNames[0]],30,400);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('filtered',hits.length,'in',ms,'ms');

  // another range query
  time = process.hrtime();
  var hits2 = filter(data[colNames[1]],20,400);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('filtered',hits2.length,'in',ms,'ms');

  // some boolean operations on hits
  time = process.hrtime();
  var u = union(hits,hits2);
  var n = intersect(hits,hits2);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('union',u.length,'intersection',n.length,'in',ms,'ms');

  // conditional 2d distribution
  time = process.hrtime();
  var x2 = dist2D(data[colNames[2]],data[colNames[3]],u);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('conditional dist2D in',ms,'ms');

  // apply a filter (via null mask)
  time = process.hrtime();
  var notU = complement(u,data[colNames[0]].v.length);
  var f2 = {
    v: data[colNames[2]].v,
    nulls: union(data[colNames[2]].nulls,notU)
  };
  calcDist(f2,50);
  diff = process.hrtime(time);
  ms = diff[0] * 1e3 + diff[1]/1e6;
  console.log('applied a filter in',ms,'ms');
});

function preprocess(data,nbins) {
  for (var axis in data) {
    calcDist(data[axis],nbins);
  }
}

function calcDist(column,nbins) {
  var values = column.v;
  var nulls = column.nulls;
  var j=0;
  column.min=Number.MAX_VALUE;
  column.max=Number.MIN_VALUE;
  for (var i=0; i<values.length; i++) {
    if (j<nulls.length && nulls[j] === i) { j++; }
    else {
      // non-null
      if (values[i] < column.min) column.min = values[i];
      if (values[i] > column.max) column.max = values[i];          
    }
  }
  // now we have min and max values
  var binSize = (column.max - column.min + 0.000001)/nbins;
  if (binSize === 0) {binSize = 1; nbins=1};
  column.binSize = binSize;
  column.bins = [];
  for(var i=0;i<nbins;i++) { column.bins.push([]); }
  column.nbins = nbins;
  // iterate through the values again and populate the histogram bins
  j=0;
  for (var i=0; i<values.length; i++) {
    if (j<nulls.length && nulls[j] === i) { j++; }
    else {
      // non-null
      var bin = Math.floor((values[i] - column.min)/binSize);
      column.bins[bin].push(i);
    }
  }
  // sort indices within each bin by value
  for (var b=0;b<nbins; b++) {
    if (column.bins[b].length > 1) {
      column.bins[b] = column.bins[b].sort(function(a,b) {
        return values[a] - values[b];
      });
    }
  }
}

function filter(column,min,max) {
  var hits = [];
  // use bins to limit the search to boundary bins
  // fully covered bins get concatenated
  if (min < column.min) min = column.min;
  if (max > column.max) max = column.max;
  var minbin = Math.floor((min - column.min)/column.binSize);
  var maxbin = Math.floor((max - column.min)/column.binSize);
  if (minbin === maxbin) {
    // find first value >= min
    // keep everything until value > max
    if (column.bins[minbin].length) {
      var i=0;
      var a = column.bins[minbin];
      while (i<a.length && column.v[a[i]] < min) {
        i++;
      }
      while (i<a.length && column.v[a[i]] <= max) {
        hits.push(a[i]);
        i++;
      }
    }
  }
  else {
    // 1. find first value in minbin >= min
    //    take that and anything after it
    if (column.bins[minbin].length) {
      var i=0;
      var a = column.bins[minbin];
      while (i<a.length && column.v[a[i]] < min) { i++; }
      while (i<a.length) { hits.push(a[i]); i++; }
    }
    // 2. iterate over all covered bins  (minbin < bin < maxbin)
    //    all values are hits
    for(var bin = minbin+1; bin < maxbin; bin++) {
      for(var i=0;i<column.bins[bin].length;i++) {
        hits.push(column.bins[bin][i]);
      }
    }
    // 3. take all values in maxbin that are <= max
    if (column.bins[maxbin].length) {
      var i=0;
      var a = column.bins[maxbin];
      while (i<a.length && column.v[a[i]] <= max) {
        hits.push(a[i]);
        i++;
      }
    }
  }
  return hits.sort(function(a,b){return a-b;});
}

function intersect(a, b) {
  var res = [];
  var i=0;
  var j=0;
  while (i<a.length && j<b.length) {
    if (a[i] < b[j]) {
      i++;
    }
    else if (a[i] > b[j]) {
      j++;
    }
    else {
      res.push(a[i]);
      i++;j++;
    }
  }
  return res;
}

function union(a,b) {
  var res = [];
  var i=0;
  var j=0;
  while (i<a.length && j < b.length) {
    if (a[i] < b[j]) { res.push(a[i]); i++; }
    else if (a[i] > b[j]) { res.push(b[j]); j++; }
    else { res.push(a[i]); i++; j++; }
  }
  while (i<a.length) {res.push(a[i]);i++;}
  while (j<b.length) {res.push(b[j]);j++;}
  return res;
}

function complement(a,n) {
  var res = [];
  var i=0;
  var j=0;
  while (i<a.length) {
    if (a[i] === j) {
      i++;
    }
    else {
      res.push(j);
    }
    j++;
  }
  while (j<n) {
    res.push(j);
    j++;
  }
  return res;
}
function dist2D(col1, col2, mask) {
  if (mask) return cdist2D(col1,col2,mask);
  var dist = {bin:[],count:[]};
  for (var i=0; i<col1.nbins; i++) {
    if (col1.bins[i].length > 0) {
      for (var j=0; j<col2.nbins; j++) {
        if (col2.bins[j].length > 0) {
          // intersect sorted integer offsets
          var bin2d = i*col2.nbins + j;
          count = 0;
          var ii = 0;
          var jj = 0;
          while (ii < col1.bins[i].length && jj < col2.bins[j].length) {
            if (col1.bins[i][ii] < col2.bins[j][jj]) {
              ii++;
            }
            else if (col1.bins[i][ii] > col2.bins[j][jj]) {
              jj++;
            }
            else {
              count++;
              ii++;jj++;
            }
          }
          if (count > 0) {
            dist.count.push(count);
            dist.bin.push(bin2d);
          }
        }
      }
    }
  }
  return dist;
}

function cdist2D(col1, col2, mask) {
  // mask is a sorted list of positions
  // turn it into an array
  var inMask = new Int8Array(col1.v.length);
  for(var i=0;i<mask.length;i++) {
    inMask[mask[i]]=1;
  }
  var dist = {bin:[],count:[]};
  for (var i=0; i<col1.nbins; i++) {
    if (col1.bins[i].length > 0) {
      for (var j=0; j<col2.nbins; j++) {
        if (col2.bins[j].length > 0) {
          // intersect sorted integer offsets
          var bin2d = i*col2.nbins + j;
          count = 0;
          var ii = 0;
          var jj = 0;
          while (ii < col1.bins[i].length && jj < col2.bins[j].length) {
            if (col1.bins[i][ii] < col2.bins[j][jj]) {
              ii++;
            }
            else if (col1.bins[i][ii] > col2.bins[j][jj]) {
              jj++;
            }
            else {
              if (inMask[col1.bins[i][ii]]) count++;
              ii++;jj++;
            }
          }
          if (count > 0) {
            dist.count.push(count);
            dist.bin.push(bin2d);
          }
        }
      }
    }
  }
  return dist;
}