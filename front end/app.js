var express = require("express");
var app = express();

var bodyParser = require("body-parser");
var CouchDB = require("node-couchdb");

//CouchDB config
var couch = new CouchDB({
	auth: {
		user:"admin",
		password:"admin"
	}
});

var dbnameS1 = "scenario1_new";
var dbnameS2 = "scenario2_new";
var dbnameS3 = "scenario3_new";

var viewUrl = "_design/s1/_view/cities";
var viewUrl2 = "_design/s2/_view/airlines";
var viewUrl3 = "_design/s3/_view/venues";
//Configuration
app.use(bodyParser.urlencoded({extended:true}));
app.use(express.static(__dirname + "/public"));

app.set("view engine", "ejs");

//RESTful 
app.get("/", function(req, res){
	res.render("landing");
});

//home page
app.get("/home", function(req, res){
	res.render("home");
})

//Scenario1 page
app.get("/home/happy", function(req, res){
	couch.get(dbnameS1, viewUrl).then(
		function(data, headers, status){
			res.render("happy", {data:data.data.rows});
		},
		function(err){
			res.send(err); 
		});
});

//Scenario2 page
app.get("/home/airline", function(req, res){
	couch.get(dbnameS2, viewUrl2).then(
		function(data, headers, status){
			res.render("airline", {data:data.data.rows});
		},
		function(err){
			res.send(err);
		});
})

app.get("/home/venues", function(req, res){
	couch.get(dbnameS3, viewUrl3).then(
		function(data, header, status){
			res.render("venues", {data:data.data.rows});
		},
		function(err) {
			res.send(err);
		});
})

//Listen port 8080
app.listen(8080, function(){
	console.log("Magic Begins");
});