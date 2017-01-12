(function() {
  'use strict';
	
  // Create the connector object
	var myConnector = tableau.makeConnector();

  // Init function for connector
  myConnector.init = function init(initCallback) {
    // tableau.log('Cookies =', Cookies.get());
    console.log('init() tableau =', tableau);
    tableau.authType = tableau.authTypeEnum.custom;

    // If we are in the auth phase we only want to show the UI needed for auth
    if (tableau.phase === tableau.phaseEnum.authPhase) {
      $("#connectorForm").css('display', 'none');
    }

    if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
      // If API that WDC is using has an enpoint that checks
      // the validity of an access token, that could be used here.
      // Then the WDC can call tableau.abortForAuth if that access token
      // is invalid.
    }

    var accessToken = Cookies.get('accessToken');
    console.log('Access token is =', accessToken);
    var hasAuth = (accessToken && accessToken.length > 0) || tableau.password.length > 0;
    updateUIWithAuthState(hasAuth);

    initCallback();

    // If we are not in the data gathering phase, we want to store the token
    // This allows us to access the token in the data gathering phase
    if (tableau.phase === tableau.phaseEnum.interactivePhase || tableau.phase === tableau.phaseEnum.authPhase) {
      if (hasAuth) {
        tableau.password = accessToken;

        if (tableau.phase === tableau.phaseEnum.authPhase) {
          console.log('init() tableau.phase == authPahse, run tableau.submit()!');
          // Auto-submit here if we are in the auth phase
          tableau.submit();
        }

        return;
      }
    }
  };

  myConnector.getSchema = function(schemaCallback) {
    var config = JSON.parse(tableau.connectionData);
    if (config.customSql) {
      info("Executing customSql: "+JSON.stringify(config.customSql));
      new Promise(function (resolve, reject) {
        sendSQLToFusion(config.fusionUrl, config.customSql, function (json) {
          resolve(json);
        });
      }).then(function (data) {
        loadTables(config.fusionUrl, schemaCallback);
      });
    } else if (config.customTable) {
      info("Creating customTable: "+JSON.stringify(config.customTable));
      // create a new DataAsset in the Catalog API using the custom query provided by the user
      new Promise(function (resolve, reject) {
        postToCatalogAPI(config.fusionUrl, "/catalog/fusion/assets", config.customTable, function (json) {
          resolve(json);
        });
      }).then(function (data) {
        loadTables(config.fusionUrl, schemaCallback);
      });
    } else {
      loadTables(config.fusionUrl, schemaCallback);
    }
	};

	myConnector.getData = function(table, doneCallback) {
    var config = JSON.parse(tableau.connectionData);
    var maxRows = config.maxRows;
		getFromCatalogAPI(config.fusionUrl, "/assets/"+table.tableInfo.id+"/rows?rows="+maxRows, function(data) {
			var obj = JSON.parse(data);
			var tableData = [];
			for (var i = 0; i < obj.length; i++) {
        tableEntry = {};
				var ref = obj[i];
				// We can use this handy shortcut because our JSON column names match our schema's column names perfectly
				Object.getOwnPropertyNames(ref).forEach(function(val, idx, array) {
          // Tab doesn't like arrays ;-)
          if (Array.isArray(ref[val])) {
            tableEntry[val] = ref[val].join(',');
          } else {
            tableEntry[val] = ref[val];
          }
				});
				tableData.push(tableEntry);
			}
			// Once we have all the data parsed, we send it to the Tableau table object
			table.appendRows(tableData);
			doneCallback();
		});
	};

	tableau.registerConnector(myConnector);

  // Called when web page first loads
  $(document).ready(function() {
    console.log('ready() Cookies =', Cookies.get());
    var accessToken = Cookies.get("accessToken");
    var hasAuth = accessToken && accessToken.length > 0;
    updateUIWithAuthState(hasAuth);

    // Login button click event
    $('#submitLoginButton').click(function() {
      var url = $('#fusionUrl').val();
      var username = $('#fusionUsername').val();
      var password = $('#fusionPassword').val();
      console.log('url =', url, ', username =', username, ', password =', password);
      doAuth(url, username, password);
    });

    // TEST Get Session button
    $('#getSessionButton').click(function() {
      // var req = $.get('http://localhost:8764/api/session');
      var req = $.ajax({
        method: 'GET',
        url: 'http://localhost:8764/api/session',
        xhrFields: {
          withCredentials: true
        }
      });

      req.done(function(data) {
        console.log('getSession done data =',data);
      });

      req.fail(function(data) {
        console.log('getSession fail data =', data);
      });
    });


    $("#submitButton").click(function() {
      var config = {};

      var fusionUrl = $('#fusionUrl').val().trim();
      if (!fusionUrl) {
        fusionUrl = "localhost:8765/api/v1";
      }
      config["fusionUrl"] = fusionUrl;

      var customQueryName = $('#customQueryName').val().trim();
      var customQuery = $('#customQuery').val().trim();
      if (customQuery) {

        if (!customQueryName) {
          // TODO: better form validation here
          alert("Must provide a name for caching the results of your custom query!");
          return;
        }

        // collapse all ws into single space
        var query = customQuery.replace(/\s\s+/g, ' ');
        // user wants to execute a custom query (Solr, streaming expression, SQL) and cache the results for analysis
        var queryLc = query.toLowerCase();
        var restOfQuery = query;

        // Determine whether it is a SQL query or Solr streaming expression
        if (queryLc.indexOf("select ") == 0) {
          // custom SQL query here ...
          config["customSql"] = {sql:query, cacheResultsAs:customQueryName};
        } else {
          var key = "collection=";

          // TODO: I suck at JS regex, but that would be better than this manual search and replace
          var collection = null;
          var at = query.indexOf(key);
          var isExpr = false;
          if (at != -1) {
            var ampAt = query.indexOf("&", at+key.length);
            if (ampAt != -1) {
              collection = query.substring(at+key.length, ampAt);
              restOfQuery = query.replace(key+collection+"&", "");
            } else {
              collection = query.substring(at+key.length);
              restOfQuery = query.replace(key+collection, "");
            }
          } else {
            // collection param not found ... but if this is a streaming expression, we can detect it
            key = "search(";
            at = query.indexOf(key);
            if (at != -1) {
              var commaAt = query.indexOf(",", at+key.length);
              if (commaAt != -1) {
                collection = query.substring(at+key.length, commaAt);
                isExpr = true;
              }
            }
          }

          if (!collection) {
            alert("Custom query must include the Fusion collection name param, e.g. 'collection=<NAME>&'");
            return;
          }

          var options = ["collection -> "+collection]
          var dataAsset = {
            projectId: "fusion",
            name: customQueryName,
            assetType: "table",
            format: "solr",
            description: "Created by the Fusion Web Data Connector for Tableau Public.",
            options: options
          };
          if (restOfQuery.indexOf("expr=") == 0) {
            // streaming expression here ...
            options.push("expr -> " + restOfQuery.substring(5));
          } else if (isExpr) {
            options.push("expr -> " + query);
          } else {
            // just a solr query here ...
            options.push("query -> "+restOfQuery);
          }
          config["customTable"] = dataAsset;
        }
      }

      var maxRows = $('#maxRows').val().trim();
      config["maxRows"] = (maxRows == "") ? 10000 : parseInt(maxRows);

      tableau.connectionName = "Lucidworks Fusion";

      var configJson = JSON.stringify(config);
      tableau.connectionData = configJson;
      info("connectionData: "+configJson);
      tableau.submit();
    });
  });

  // This function togglels the label shown depending
  // on whether or not the user has been authenticated
  function updateUIWithAuthState(hasAuth) {
    if (hasAuth) {
      $('#loginForm').css('display', 'none');
      $('#connectorForm').css('display', 'block');
    } else {
      $('#loginForm').css('display', 'block');
      $('#connectorForm').css('display', 'none');
    }
  }

  // An on-click function for login to Fusion
  function doAuth(fusionUrl, username, password) {
    // Use Fusion Sessions API to create a session
    // fusionUrl += '/api/session?realmName=native';
    fusionUrl += '/api/session';
    var sessionData = {
      username: username,
      password: password
    };
    console.log('fusionUrl =', fusionUrl);
    var loginRequest = $.ajax({
      method: 'POST',
      url: fusionUrl,
      data: JSON.stringify(sessionData),
      // data: sessionData,  // this cause 400 error Malform JSON
      // dataType: 'text',
      processData: false,  // do not transform the data into query string, otherwise it'll cause 400 error.
      contentType: 'application/json',
      // headers: {
      //   'content-type': 'application/json',
      //   'cache-control': 'no-cache'
      // },
      // mimeType: 'application/json',
      // cache: false,
      // async: true,
      crossDomain: true,
      // jsonp: false,
      xhrFields: {
        withCredentials: true
      }
    });
    // var loginRequest = $.ajax({
    //   method: 'GET',
    //   url: fusionUrl
    // });

    loginRequest.done(function success(data, status, respObj) {
      console.log('success data =', data);
      console.log('status =', status);
      console.log('respObj =', respObj);
      console.log('respObj.getAllResponseHeaders() =', respObj.getAllResponseHeaders());
      console.log('Cookies.get() =', Cookies.get());
    });

    loginRequest.fail(function fail(data) {
      console.log('fail data =', data);
    });
  }

  function log(lvl, msg) {
    var logMsg = lvl+": "+msg;
    console.log(logMsg);
  }

  function info(msg) {
    log("INFO", msg);
  }

  function error(msg) {
    log("ERROR", msg);
  }

  function warn(msg) {
    log("WARN", msg);
  }

  function buildFusionCallUrl(fusionUrl, path) {
    return "http://localhost:8889/"+fusionUrl+path;
  }

  function getFromCatalogAPI(fusionUrl, path, cb) {
    var obj = new XMLHttpRequest();
    obj.overrideMimeType("application/json");
    var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion" + path);
    info("Sending GET request to: "+callUrl);
    obj.open("GET", callUrl, true);
    obj.onreadystatechange = function() {
      if (obj.readyState == 4 && obj.status == "200") {
        cb(obj.responseText);
      } else if (obj.readyState == 4 && obj.status != "200") {
        error(obj.status+": "+obj.responseText);
      }
    }
    obj.send(null);
  }

  function postToCatalogAPI(fusionUrl, path, toPost, cb) {
    var obj = new XMLHttpRequest();
    obj.overrideMimeType("application/json");
    var callUrl = buildFusionCallUrl(fusionUrl, path);
    obj.open("POST", callUrl, true);
    obj.setRequestHeader("Content-type", "application/json");
    obj.onreadystatechange = function() {
      if (obj.readyState == 4 && obj.status == "200"){
        cb(obj.responseText);
      } else if (obj.readyState == 4 && obj.status != "200") {
        error(obj.status+": "+obj.responseText);
      }
    };
    var jsonReq = JSON.stringify(toPost)
    info("POSTing JSON request to Fusion Catalog: "+jsonReq+" at: "+callUrl);
    obj.send(jsonReq);
  }

  function describeTable(fusionUrl, conn) {
    var tableName = conn["id"];
    var tableSchemaPath = "/assets/" + tableName + "/schema";

    return new Promise(function(resolve, reject) {
      getFromCatalogAPI(fusionUrl, tableSchemaPath, function (json) {
        var obj = JSON.parse(json);
        var table = {
          id: tableName,
          alias: tableName,
          columns: []
        }
        var props = obj.properties
        for (var col in props) {
          if (props.hasOwnProperty(col)) {
            var colType = props[col]["type"];
            var dataType = tableau.dataTypeEnum.string;
            if (colType == "integer") {
              dataType = tableau.dataTypeEnum.int
            } else if (colType == "number" || colType == "float" || colType == "double") {
              dataType = tableau.dataTypeEnum.float
            } else if (colType == "string") {
              var format = props[col]["format"]
              if (format == "date-time") {
                dataType = tableau.dataTypeEnum.datetime
              }
            }
            table.columns.push({id: col, alias: col, dataType: dataType})
          }
        }
        resolve(table);
      });
    });
  }

  function sendSQLToFusion(fusionUrl, sql, cb) {
    var obj = new XMLHttpRequest();
    obj.overrideMimeType("application/json");
    var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion/query");
    obj.open("POST", callUrl, true);
    obj.setRequestHeader("Content-type", "application/json");
    obj.onreadystatechange = function () {
      if (obj.readyState == 4 && obj.status == "200") {
        cb(obj.responseText);
      } else if (obj.readyState == 4 && obj.status != "200") {
        error(obj.status+": "+obj.responseText);
      }
    };
    obj.send(JSON.stringify(sql));
  }

  function loadTables(fusionUrl, schemaCallback) {
    new Promise(function(resolve, reject) {
      sendSQLToFusion(fusionUrl, { sql:"show tables in default" }, function(json) {
        var tables = [];
        JSON.parse(json).forEach(function(t){tables.push({id:t["tableName"], alias:t["tableName"]})});
        tables.sort(function(lhs,rhs){return lhs.id.localeCompare(rhs.id)});
        resolve(tables);
      });
    }).then(function(data) {
      var schemas = [];
      data.forEach(function(c){schemas.push(describeTable(fusionUrl, c))});
      Promise.all(schemas).then(function(data) {
        schemaCallback(data);
      });
    });
  }
})();


