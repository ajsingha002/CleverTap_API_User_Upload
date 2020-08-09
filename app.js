const request = require('request');
const csv = require('csv-parser');
const fs = require('fs');

const headers = {
    'X-CleverTap-Account-Id': 'RK4-R6K-565Z',
    'X-CleverTap-Passcode': '40c575aa496e4adca295560d38f9e341',
    'Content-Type': 'application/json; charset=utf-8'
};

//const dataString = '{"d":[{"objectId":"ARGHATEST0000002","type":"profile","profileData":{"User ID": "19720A2F-4D56-3615-8EE7-6AJAJ2B31023", "Location": "Mumbai", "Customer Type": "L", "Subcription ID": "B53B21CF-951B-7E01-5A61-CCEE7CAJAJAJ", "Gender": "Male"}},{"objectId":"ARGHATEST0000003","type":"profile","profileData":{"User ID": "19720A2F-4D56-3615-8EE7-6AJAJ2B31023", "Location": "Mumbai", "Customer Type": "L", "Subcription ID": "B53B21CF-951B-7E01-5A61-CCEE7CAJAJAJ", "Gender": "Male"}}]}';

const options = {
    url: 'https://api.clevertap.com/1/upload',
    method: 'POST',
    headers: headers,
    body: {}
};

function callback(error, response, body) {
    if (!error && response.statusCode == 200) {
        console.log(body);
    }
}

//dataStringArray containing indivisual datastrings for User Profiles
let dataStringArray = [];
//add iteration code - read CSV file for 100 records and make datastring
let getDataStringArray = () => {
    fs.createReadStream('data.csv')
  .pipe(csv())
  .on('data', (row) => {
    //console.log(row);
    let dataString  = {
        objectId : row["User ID"],
        type : "profile",
        profileData : {
            "Location" : row.Location,
            "Customer Type" : row["Customer Type"],
            "Subcription ID" : row["Subcription ID"],
            "Gender" : row.Gender
        }
    }
    dataStringArray.push(dataString);
  })
  .on('end', () => {
    console.log('CSV file successfully processed');
    sendUserProfileCalls();
  });
}

getDataStringArray();

//iterator for bulking 100 User Profile Calls together
let sendUserProfileCalls = () => {
    console.log("Sending Calls");
    let req = {
        d: []
    };
    //call counter
    let counter = 0;
    for(let i = 0; i < dataStringArray.length; i++) {
        req.d.push(dataStringArray[i]);
        counter++;
        if(counter%100===0 || i===dataStringArray.length-1) {
            //make http call
            options.body = JSON.stringify(req);
            //console.log(options);
            request(options, callback);
            //delay 3 sec
            //setTimeout(function() {}, 3000);
            //empty array for next bulking
            req.d = [];
            //console.log("\n\n\n\n");
        }
    }
}
