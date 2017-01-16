(function() {
  'use strict';
  
  var API_APOLLO = '/api/apollo';
  var API_V1 = '/api/v1';

  // Create the connector object
  var myConnector = tableau.makeConnector();

  // Init function for connector
  myConnector.init = function init(initCallback) {
    console.log('init() tableau =', tableau);
    tableau.authType = tableau.authTypeEnum.custom;

    // If we are in the auth phase we only want to show the UI needed for auth
    // if (tableau.phase === tableau.phaseEnum.authPhase) {
    //   $("#connectorForm").css('display', 'none');
    // }

    if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
      // If API that WDC is using has an enpoint that checks
      // the validity of an access token, that could be used here.
      // Then the WDC can call tableau.abortForAuth if that access token
      // is invalid.
    }

    // var accessToken = Cookies.get('accessToken');
    // console.log('Access token is =', accessToken);
    // var hasAuth = (accessToken && accessToken.length > 0) || tableau.password.length > 0;
    // updateUIWithAuthState(hasAuth);

    initCallback();

    // If we are not in the data gathering phase, we want to store the token
    // This allows us to access the token in the data gathering phase
    if (tableau.phase === tableau.phaseEnum.interactivePhase || tableau.phase === tableau.phaseEnum.authPhase) {      
      // if (hasAuth) {
      //   tableau.password = accessToken;
      //
      //   if (tableau.phase === tableau.phaseEnum.authPhase) {
      //     console.log('init() tableau.phase == authPahse, run tableau.submit()!');
      //     // Auto-submit here if we are in the auth phase
      //     tableau.submit();
      //   }
      //
      //   return;
      // }
    }
  };

  myConnector.getSchema = function(schemaCallback) {
    console.info('getSchema()');
    var config = JSON.parse(tableau.connectionData);
    if (config.customSql) {
      console.info("Executing customSql: "+JSON.stringify(config.customSql));
      new Promise(function (resolve, reject) {
        sendSQLToFusion(config.fusionUrl, config.customSql, function (json) {
          resolve(json);
        });
      }).then(function (data) {
        loadTables(config.fusionUrl, schemaCallback);
      });
    } else if (config.customTable) {
      console.info("Creating customTable: "+JSON.stringify(config.customTable));
      // create a new DataAsset in the Catalog API using the custom query provided by the user
      new Promise(function (resolve, reject) {
        postToCatalogAPI(config.fusionUrl, "/catalog/fusion/assets", config.customTable, function (json) {
          resolve(json);
        });
      }).then(function (data) {
        loadTables(config.fusionUrl, schemaCallback);
      });
    } else {
      console.log('getSchema() basic case, no customSql or customTable');
      loadTables(config.fusionUrl, schemaCallback);
    }
  };

  myConnector.getData = function(table, doneCallback) {
    var config = JSON.parse(tableau.connectionData);
    var maxRows = config.maxRows;
    var cols = table.tableInfo.columns.map(function(c) {
      // tab gives us back the field ids encoded
      return {"id":decodeFieldId(c["id"]), "enc":c["id"]};
    });
    var tableName = table.tableInfo.id;
    var url = buildFusionCallUrl(config.fusionUrl, "/catalog/fusion/assets/"+tableName+"/rows?rows="+maxRows);
    info("Loading up to "+maxRows+" rows for table "+tableName+" with GET to: "+url);
    oboe(url).node('![*]', function(row) {
      var tabRow = {};
      for (var c=0; c < cols.length; c++) {
        var col = cols[c]["id"];
        var enc = cols[c]["enc"];
        var colData = row[col];
        if (colData) {
          tabRow[enc] = Array.isArray(colData) ? colData.join(',') : colData;
        } else {
          tabRow[enc] = null; // this avoids Tableau logging about a missing column entry in the row
        }
      }
      return tabRow;
    }).done(function(tableData) {
      table.appendRows(tableData);
      doneCallback();
    }).fail(function(err) {
      tableau.abortWithError("Load data for "+tableName+" failed due to: ("+err.statusCode+") "+err.body);
    });
  };

  tableau.registerConnector(myConnector);

  // Called when web page first loads
  $(document).ready(function() {
    // Use CORS proxy checkbox
    $('#useCorsProxy').change(function() {
      if (this.checked) {
        $('#fusionUrl').val('http://localhost:8889/localhost:8765');
      } else {
        $('#fusionUrl').val('http://localhost:8764');
      }
    });

    // Verify Login button
    $('#verifyLoginButton').click(function() {
      var url = $('#fusionUrl').val();
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();
      doAuth(url, tableau.username, tableau.password);
    });   

    // TEST Get Session button
    $('#getSessionButton').click(function() {
      var req = $.ajax({
        method: 'GET',
        url: 'http://localhost:8764/api/session',
        xhrFields: { withCredentials: true }
      });

      req.done(function(data) {
        console.log('getSession done data =',data);
      });

      req.fail(function(data) {
        console.log('getSession fail data =', data);
      });
    });

    // Load Fusion Tables button
    $("#submitButton").click(function() {
      var config = {};
      
      config.useCorsProxy = $('#useCorsProxy').prop('checked');      
      var fusionUrl = $('#fusionUrl').val().trim();
      if (!fusionUrl) {
        fusionUrl = "localhost:8765/api/v1";
      }
      config.fusionUrl = fusionUrl;
      config.fusionUsername = $('#fusionUsername').val();
      config.fusionPassword = $('#fusionPassword').val();
      // Store credentials in tableau for easy access later
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();

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
        if (queryLc.indexOf("select ") === 0) {
          // custom SQL query here ...
          config.customSql = {sql:query, cacheResultsAs:customQueryName};
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

          var options = ["collection -> " + collection];
          var dataAsset = {
            projectId: "fusion",
            name: customQueryName,
            assetType: "table",
            format: "solr",
            description: "Created by the Fusion Web Data Connector for Tableau Public.",
            options: options
          };
          if (restOfQuery.indexOf("expr=") === 0) {
            // streaming expression here ...
            options.push("expr -> " + restOfQuery.substring(5));
          } else if (isExpr) {
            options.push("expr -> " + query);
          } else {
            // just a solr query here ...
            options.push("query -> "+restOfQuery);
          }
          config.customTable = dataAsset;
        }
      }

      var maxRows = $('#maxRows').val().trim();
      config.maxRows = (maxRows === "") ? 10000 : parseInt(maxRows);
      config.changedOn = new Date(); // this ensures Tableau always refreshes the table list from the server on edit

      var configJson = JSON.stringify(config);
      tableau.connectionData = configJson;
      console.info("connectionData: "+configJson);
      tableau.connectionName = "Lucidworks Fusion";
      tableau.submit();
    });
  });

  // An on-click function for login to Fusion
  function doAuth(fusionUrl, username, password) {
    // Use Fusion Sessions API to create a session
    // fusionUrl += '/api/session?realmName=native';
    fusionUrl += '/api/session';

    var sessionData = {
      username: username,
      password: password
    };
    
    console.log('fusionUrl, username, password =', fusionUrl, username, password);

    var loginPromise = $.ajax({
      method: 'POST',
      url: fusionUrl,
      data: JSON.stringify(sessionData),
      // data: sessionData,  // this cause 400 error Malform JSON
      processData: false,  // do not transform the data into query string, otherwise it'll cause 400 error.
      contentType: 'application/json',
      crossDomain: true,
      xhrFields: { withCredentials: true }
    });

    loginPromise.done(function success(data, status, respObj) {
      console.log('Login successful status =', status);
    });

    loginPromise.fail(function fail(err) {
      console.error('Error authenticating to Fusion, error =', err);
    });

    return loginPromise;
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
    var config = JSON.parse(tableau.connectionData);
    var url;
    if (config.useCorsProxy) {
      url = fusionUrl + API_V1 + path;
    } else {
      url = fusionUrl + API_APOLLO + path;
    }
    return url;
  }

  // function getFromCatalogAPI(fusionUrl, path, cb) {
  //   var obj = new XMLHttpRequest();
  //   obj.overrideMimeType("application/json");
  //   var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion" + path);
  //   info("Sending GET request to: "+callUrl);
  //   obj.open("GET", callUrl, true);
  //   obj.onreadystatechange = function() {
  //     if (obj.readyState == 4 && obj.status == "200") {
  //       cb(obj.responseText);
  //     } else if (obj.readyState == 4 && obj.status != "200") {
  //       error(obj.status+": "+obj.responseText);
  //     }
  //   };
  //   obj.send(null);
  // }
  function getFromCatalogAPI(fusionUrl, path) {
    var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion" + path);
    return $.ajax({
      method: 'GET',
      url: callUrl,
      xhrFields: { withCredentials: true }
    });
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
    var tableName = conn.id;
    var tableSchemaPath = "/assets/" + tableName + "/schema";
    // return new Promise(function(resolve, reject) {
    //   getFromCatalogAPI(fusionUrl, tableSchemaPath, function (json) {
    //     var obj = JSON.parse(json);
    //     var table = {
    //       id: tableName,
    //       alias: tableName,
    //       columns: []
    //     };
    //     var props = obj.properties;
    //     for (var col in props) {
    //       if (props.hasOwnProperty(col)) {
    //         var colType = props[col]["type"];
    //         var dataType = tableau.dataTypeEnum.string;
    //         if (colType == "integer" || colType == "int") {
    //           dataType = tableau.dataTypeEnum.int;
    //         } else if (colType == "number" || colType == "float" || colType == "double") {
    //           dataType = tableau.dataTypeEnum.float;
    //         } else if (colType == "string") {
    //           var format = props[col]["format"];
    //           if (format == "date-time") {
    //             dataType = tableau.dataTypeEnum.datetime;
    //           }
    //         }
    //         // tab public doesn't like dots in the field names
    //         table.columns.push({id: encodeFieldId(col), alias: col, dataType: dataType});
    //       }
    //     }
    //     resolve(table);
    //   });
    // });
    return getFromCatalogAPI(fusionUrl, tableSchemaPath)
      .then(function(data) {
        console.log('getFromCatalogAPI data =', data);
        // var obj = JSON.parse(data);
        var obj = data;
        var table = {
          id: tableName,
          alias: tableName,
          columns: []
        };
        var props = obj.properties;
        for (var col in props) {
          if (props.hasOwnProperty(col)) {
            var colType = props[col].type;
            var dataType = tableau.dataTypeEnum.string;
            if (colType === 'integer' || colType === 'int') {
              dataType = tableau.dataTypeEnum.int;
            } else if (colType === 'number' || colType === 'float' || colType === 'double') {
              dataType = tableau.dataTypeEnum.float;
            } else if (colType === 'string') {
              var format = props[col].format;
              if (format === 'date-time') {
                dataType = tableau.dataTypeEnum.datetime;
              }
            }
            // tab public doesn't like dots in the field names
            table.columns.push({id: encodeFieldId(col), alias: col, dataType: dataType});
          }
        }

        return table;
      }, function(err) {
        console.error('Error getting data from Catalog API, err =', err);
      });
  }

  // function sendSQLToFusion(fusionUrl, sql, cb) {
  //   var obj = new XMLHttpRequest();
  //   obj.overrideMimeType("application/json");
  //   var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion/query");
  //   obj.open("POST", callUrl, true);
  //   obj.setRequestHeader("Content-type", "application/json");
  //   obj.onreadystatechange = function () {
  //     if (obj.readyState == 4 && obj.status == "200") {
  //       cb(obj.responseText);
  //     } else if (obj.readyState == 4 && obj.status != "200") {
  //       error(obj.status+": "+obj.responseText);
  //     }
  //   };
  //   obj.send(JSON.stringify(sql));
  // }
  function sendSQLToFusion(fusionUrl, sql) {
    var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion/query");   
    console.log('callUrl, sql =', callUrl, sql);

    var req = $.ajax({
      method: 'POST',
      url: callUrl,
      data: JSON.stringify(sql),
      processData: false,
      contentType: 'application/json',
      crossDomain: true,
      xhrFields: { withCredentials: true }
    })
    .then(function(data) {
      console.log('sendSQLToFusion success data =', data);
      return data;
    }, function(err) {
      console.log('sendSQLToFusion err =', err);
      // if err === 401 Unauthorized, try to perform auth
      if (err.status === 401) {
        console.warn('Unauthorized request, trying to login with the input username and password...');
        var retryPromise = doAuth(fusionUrl, tableau.username, tableau.password)
          .then(function() {
            // Resend the SQL query request
            console.info('Resending SQL query...');
            $.ajax({
              method: 'POST',
              url: callUrl,
              data: JSON.stringify(sql),
              processData: false,
              contentType: 'application/json',
              crossDomain: true,
              xhrFields: { withCredentials: true }
            })
            .then(function(data) {
              console.log('Resent SQL query successfully, data =', data);
              return $.Deferred().resolve(data);
            }, function(err) {
              console.error('Error resending the SQL query, error =', err);
              return $.Deferred().reject(err);
            });
          });
        // return $.Deferred().resolve(retryPromise);
      } else {
        console.error('Error sending SQL query, error =', err);
        tableau.abortWithError("Failed to execute SQL ["+sql+"] due to: ("+err.status+") "+err);
      }
    });

    return req;
  }

  function encodeFieldId(fieldName) {
    return fieldName.replace(/\./g,"_DOT_");
  }

  function decodeFieldId(fieldId) {
    return fieldId.replace(/_DOT_/g,".");
  }

  function loadTables(fusionUrl, schemaCallback) {
    // new Promise(function(resolve, reject) {
    //   sendSQLToFusion(fusionUrl, { sql:"show tables in default" }, function(json) {
    //     var tables = [];
    //     JSON.parse(json).forEach(function(t) {
    //       tables.push({id:t.tableName, alias:t.tableName});
    //     });
    //     tables.sort(function(lhs,rhs){return lhs.id.localeCompare(rhs.id);});
    //     resolve(tables);
    //   });
    // })
    // .then(function(data) {
    //   var schemas = [];
    //   data.forEach(function(c) {
    //     schemas.push(describeTable(fusionUrl, c));
    //   });
    //   Promise.all(schemas).then(function(data) {
    //     schemaCallback(data);
    //   });
    // });
    var sql = { sql:"show tables in default" };
    sendSQLToFusion(fusionUrl, sql)
    .then(function success(data) {
      console.log('first then data =', data);
      var tables = [];
      data.forEach(function(t) {
        tables.push({id:t.tableName, alias:t.tableName});
      });
      tables.sort(function(lhs,rhs){return lhs.id.localeCompare(rhs.id);});
      return tables;
    }, function failure(data) {
      console.log('failure() data =', data);

    })
    .then(function(data) {
      console.log('second then data =', data);
      var schemas = [];
      data.forEach(function(c) {
        schemas.push(describeTable(fusionUrl, c));
      });
      Promise.all(schemas).then(function(data) {
        schemaCallback(data);
      });
      // $.when(schemas).then(function(data) {
      //   console.log('schemas data =', data);
      //   schemaCallback(data);
      // });
    });
  }
})();


