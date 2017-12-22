var request = require('request');			//run command 'npm install request' if module missing error
var fs = require('fs');						//run command 'npm install fs' if module missing error

/*
	I'M USING ALL THESE FUCKING GLOBAL VARIABLES BECAUSE IT'S ANNOYING AS FUCK TO SCOPE EVERYTHING WITH ALL THESE ASYNCHRONOUS NON-BLOCKING WEB CALLS FINISHING AFTER THE REST OF MY CODE TRIES TO RUN
	
	I JUST WISH HTTP REQUESTS WERE BLOCKING SO I CAN JUST WRITE MY FUCKING CODE IN THE SEQUENCE I WANT IT TO RUN IN AND NOT DEAL WITH ALL THESE FUCKING ANONYMOUS CALLBACKS
*/
var initialUrl = "https://api.twitch.tv/kraken/streams?game=League+of+Legends&limit=25&offset=0";
var nextUrl = "";
var stringResult = "";
var streamersPolled = 0;
var currentActiveStreamers = 0;
var firstPass = true;


function theDirector(){				//master traffic cop function because of all these fucking concurrency and data integrity issues

	if (firstPass){
		twitchPull(initialUrl, compileStreamViewerCounts);
		firstPass = false;
		return;
	}
	
	if (streamersPolled < currentActiveStreamers && nextUrl != ""){
		twitchPull(nextUrl, compileStreamViewerCounts);
		return;
	}
	
	if (streamersPolled >= currentActiveStreamers && nextUrl !=""){
		writeData();
		return;
	}
};

function writeData(){
		fs.writeFile('testdata.txt', stringResult);
}

var compileStreamViewerCounts = function(twitchData, url){
	
	var jsonData = JSON.parse(twitchData);
	
	for(var x=0;x<jsonData.streams.length; x++){
		console.log(jsonData.streams[x].channel.display_name + ": " + jsonData.streams[x].viewers);
		stringResult += jsonData.streams[x].channel.display_name + ": " + jsonData.streams[x].viewers + "\n";		
	}
	
	if (url == initialUrl){ 										//if initial pass, get streamer count
		currentActiveStreamers = jsonData._total;	
	}
	
	nextUrl = jsonData._links.next;
	streamersPolled +=25;
	theDirector();
	return;
}

function twitchPull(url, dataProcessor){

	request(url, function (error, response, body) {

			console.log("Calling " + url);

		   if (!error && response.statusCode == 200) {
				dataProcessor(body, url);
				return;
			}
		   
		   else{
			 console.log("error:" + error);
			 console.log("response:" + response);
			 console.log("status_code:" + response.statusCode);
			 return;
			}
		});	 

};

theDirector();

