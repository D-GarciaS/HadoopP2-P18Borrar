var express = require("express");
var app = express();
fs = require('fs')

var geoData;

// We begin by loading the data file. As usual, this is done with a callback,
// which either receives an error (e.g., if the file was not found) or the data
// in the file, which we then remember in the 'geoData' variable. Error 34
// is 'file not found'.

fs.readFile('./part-r-00000', 'utf8', function (err,data) {
  if (err) {
    if (err.errno == 34) {
      console.log("Cannot find the file 'part-r-00000' - did you copy it from ");
      console.log("your 'output' directory to the current directory?");
      console.log("Try running 'cp ../output/part-r-00000 .'");
      process.exit(1);
    }
    
    console.log("Cannot read from 'part-r-00000': "+err);
    process.exit(1);
  }
  geoData = data;
});

// The line below tells Node to include a special header in the response that 
// tells the browser not to cache any files. That way, you do not need to 
// flush the browser's cache whenever you make changes.

var env = process.env.NODE_ENV || 'development';
if ('development' == env) {
  app.use(function(req, res, next) {
    res.setHeader("Cache-Control", "no-cache must-revalidate");
    return next();
  });
};

app.engine('.html', require('ejs').__express);
app.set('view engine', 'html');

// Here we simply say that the page 'index.ejs' should be returned whenever the 
// browser requests the main page (/)

app.get('/', function(req, res){
res.render("index.ejs");
});

// When the browser requests /getGeoData, we return the data we read earlier
// from the file part-r-00000 (the MapReduce output file).

app.get('/getGeoData', function(req, res) {
  var body = geoData;
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Content-Length', body.length);
  res.end(body);
});

// We also have a special 'page' that contains your name, so that it can be 
// displayed on the web page along with the map. This is inteded primarily
// as a sanity check for your grader, in case a file is left in the browser
// cache (we don't want to take points from you for someone else's bugs...)

app.get('/author', function(req, res) {
  var body = "Solution by: Your Name here (yourseaslogin)";
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Content-Length', body.length);
  res.end(body);
});

// Finally, we tell the web server to listen for requests on port 8080,
// and we print a little message to the console to remind the user that
// now it is time to open the main page in the browser.

app.listen(8080);
console.log("Server is running. Now open 'http://localhost:8080' in a browser.");
