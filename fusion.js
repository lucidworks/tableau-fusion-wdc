(function() {
  'use strict';
  
  var API_APOLLO = '/api/apollo';
  var API_V1 = '/api/v1';
  var selectedTables = [];  // Selected Fusion tables in the list that will be sent to Tableau

  // Create the connector object
  var myConnector = tableau.makeConnector();

  myConnector.getSchema = function(schemaCallback) {
    console.log('getSchema()');

    var config = JSON.parse(tableau.connectionData);
    if (config.customSql) {
      console.info("Executing customSql: "+JSON.stringify(config.customSql));
      postToCatalogAPI(config.fusionUrl, '/catalog/fusion/query', config.customSql)
      .fail(function() {
        console.error('Error executing customSql');
      })
      .always(function() {
        loadTables(config.fusionUrl, schemaCallback);
      });
    } else if (config.customTable) {
      console.info("Creating customTable: "+JSON.stringify(config.customTable));
      // create a new DataAsset in the Catalog API using the custom query provided by the user
      postToCatalogAPI(config.fusionUrl, '/catalog/fusion/assets', config.customTable)
      .fail(function() {
        console.error('Error creating customTable');
      })
      .always(function() {
        loadTables(config.fusionUrl, schemaCallback);
      });
    } else {
      loadTables(config.fusionUrl, schemaCallback);
    }
  };

  myConnector.getData = function(table, doneCallback) {
    console.log('getData()');

    var config = JSON.parse(tableau.connectionData);
    var maxRows = config.maxRows;
    var cols = table.tableInfo.columns.map(function(c) {
      // tab gives us back the field ids encoded
      return {"id":decodeFieldId(c.id), "enc":c.id};
    });
    var tableName = table.tableInfo.id;
    var url = buildFusionCallUrl(config.fusionUrl, "/catalog/fusion/assets/"+tableName+"/rows?rows="+maxRows);
    info("Loading up to "+maxRows+" rows for table "+tableName+" with GET to: "+url);    

    oboe({
      url: url,
      method: 'GET',
      withCredentials: true
    })
    .node('![*]', function(row) {
      var tabRow = {};
      for (var c=0; c < cols.length; c++) {
        var col = cols[c].id;
        var enc = cols[c].enc;
        var colData = row[col];
        if (colData) {
          tabRow[enc] = Array.isArray(colData) ? colData.join(',') : colData;
        } else {
          tabRow[enc] = null; // this avoids Tableau logging about a missing column entry in the row
        }
      }
      table.appendRows([tabRow]);      
      // By returning oboe.drop, the parsed JSON obj will be freed,
      // allowing it to be garbage collected.
      return oboe.drop;
    }).done(function(tableData) {
      doneCallback();
    }).fail(function(err) {
      tableau.abortWithError("Load data for "+tableName+" failed due to: ("+err.statusCode+") "+err.body);
    });
  };

  tableau.registerConnector(myConnector);

  // Called when web page first loads
  $(document).ready(function() {
    // Show Advanced Options checkbox
    // $('#showAdvanced').change(function() {
    //   if (this.checked) {
    //     $('.advanced-options').css('display', '');
    //   } else {
    //     $('.advanced-options').css('display', 'none');
    //   }
    // });
    $('#showAdvanced').change(showAdvancedOptions);

    $('#selectAllTablesCheckbox').change(toggleAllTablesCheckbox);

    // Load Tables button
    $('#loadTablesButton').click(function() {
      // Clear status labels
      $('#loadTablesSuccess').css('display', 'none');
      $('#loadTablesFail').css('display', 'none');

      var config = {};
      config.fusionUrl = $('#fusionUrl').val().trim();
      // Store credentials in tableau for easy access later
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();      
      tableau.connectionData = JSON.stringify(config);

      // Clear table list before loading
      var fusionTables = $('#fusionTables');
      fusionTables.html('');

      loadFusionTables(config.fusionUrl)
        .then(function(data) {
          // console.log('data =', data);
          var countPromises = [];
          data.forEach(function(table) {
            var totalRows;
            // Get total rows count from each table
            var promise = getFromCatalogAPI(config.fusionUrl, '/assets/' + table.tableName + '/count')
              .then(function(count) {
                // Catalog /count endpoint has two response formats, we need to check.
                if (count instanceof Array) {
                  totalRows = count[0]._c0;
                } else {
                  totalRows = count['count(1)'];
                }

              })
              .then(function() {
                var totalRowsColumnId = table.tableName + 'TotalRows';
                var filtersColumnId = table.tableName + 'Filters';
                var sampleColumnId = table.tableName + 'Sample';
                var maxRowsColumnId = table.tableName + 'MaxRows';
                var maxRows = totalRows < 10000 ? totalRows : 10000;

                // Add a row of metadata to the table list
                $('#fusionTables').append(
                  '<tr>' +
                  '<td><input class="select-table" type="checkbox" checked></td>' +
                  '<td>' + table.tableName + '</td>' +
                  '<td>Solr</td>' +
                  '<td id="' + totalRowsColumnId + '">' + totalRows + '</td>' +  // Get total rows
                  '<td>' + '<input type="text" placeholder="plot_txt_en:love" id="' + filtersColumnId + '">' + '</td>' +
                  '<td>' + '<input class="sample" type="number" min="1" max="100" placeholder="10" id="' + sampleColumnId + '">' + '</td>' +
                  '<td class="max-rows" id="' + maxRowsColumnId + '">' + maxRows + '</td>' +
                  '</tr>'
                );
                
                var totalRowsColumnObj = $('#' + totalRowsColumnId);
                var filtersColumnObj = $('#' + filtersColumnId);
                var sampleColumnObj = $('#' + sampleColumnId);
                var maxRowsColumnObj = $('#' + maxRowsColumnId);
                // Attach event listener to Sample column to compute 'Max Rows to Load' value
                sampleColumnObj.blur(function() {
                  if (sampleColumnObj.val()) {
                    // Compute 'Max Rows to Load'
                    var maxValue = Math.round(totalRowsColumnObj.text() * sampleColumnObj.val() / 100);
                    maxRowsColumnObj.text(maxValue);
                  } else {  // If sample value is undefined, set max rows to default value.
                    maxRowsColumnObj.text(maxRows);
                  }
                });
              });
            countPromises.push(promise);
          });

          // After finished loading all tables and metadata
          Promise.all(countPromises).then(function() {
            // TODO send 'finish loading table' event
            console.info('Finished loading all tables.');
            $('#loadTablesSuccess').css('display', '');
          });
        })
        .fail(function() {
          console.error('Error loading tables.');
          $('#loadTablesFail').css('display', '');
        });
    }); // End of Load Tables button

    // TESTING
    // $('#fusionTables')
    //   .on('click', '.max-rows', function() {
    //     console.log('$(this).text() =', $(this).text());
    //   })
    //   .on('blur', '.sample', function() {
    //     console.log('$(this).text() =', $(this).text());
    //   });
    // End of TESTING

    // Execute button
    $('#executeQueryButton').click(function() {
      // Clear status labels
      $('#executeQuerySuccess').css('display', 'none');
      $('#executeQueryFail').css('display', 'none');

      var config = {};
      config.fusionUrl = $('#fusionUrl').val().trim();
      // Store credentials in tableau for easy access later
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();      
      tableau.connectionData = JSON.stringify(config);

      var customQueryName = $('#customQueryName').val().trim();
      var customQuery = $('#customQuery').val().trim();

      // This if block is for parsing the customQuery
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

      if (config.customSql) {
        console.info("Executing customSql: "+JSON.stringify(config.customSql));
        postToCatalogAPI(config.fusionUrl, '/catalog/fusion/query', config.customSql)
          .done(function(data) {
            console.log('customSql data = ', data);
            $('#executeQuerySuccess').css('display', '');
          })
          .fail(function() {
            console.error('Error executing customSql');
            $('#executeQueryFail').css('display', '');
          });
          
      } else if (config.customTable) {
        console.info("Creating customTable: "+JSON.stringify(config.customTable));
        // create a new DataAsset in the Catalog API using the custom query provided by the user
        postToCatalogAPI(config.fusionUrl, '/catalog/fusion/assets', config.customTable)
          .done(function(data) {
            console.log('customTable data =', data);
            $('#executeQuerySuccess').css('display', '');
          })
          .fail(function() {
            console.error('Error creating customTable');
            $('#executeQueryFail').css('display', '');
          });
          
      } else {
        console.warn('No customSql or customTable to execute.');
        $('#executeQueryFail').css('display', '');
      }

    }); // End of Execute button

    // Done button
    $("#submitButton").click(function() {
      var config = {};
      var fusionUrl = $('#fusionUrl').val().trim();
      if (!fusionUrl) {
        fusionUrl = "http://localhost:8889/localhost:8765";
      }
      config.fusionUrl = fusionUrl;
      // Store credentials in tableau for easy access later
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();

      // TODO remove maxRows, no need anymore.
      var maxRows = $('#maxRows').val().trim();
      config.maxRows = (maxRows === "") ? 10000 : parseInt(maxRows);
      config.changedOn = new Date(); // this ensures Tableau always refreshes the table list from the server on edit

      var configJson = JSON.stringify(config);
      tableau.connectionData = configJson;
      tableau.connectionName = "Lucidworks Fusion";
      tableau.submit();
    }); // End of Done button
  }); // End of document.ready() 

  // Show Advanced Options checkbox
  function showAdvancedOptions() {
    if (this.checked) {
      $('.advanced-options').css('display', '');
    } else {
      $('.advanced-options').css('display', 'none');
    }
  }

  // Toggle the all tables checkbox for the table list
  function toggleAllTablesCheckbox() {
    if (this.checked) {
      $('.select-table').prop('checked', true);
    } else {
      $('.select-table').prop('checked', false);
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

    return $.ajax({
      method: 'POST',
      url: fusionUrl,
      data: JSON.stringify(sessionData),
      processData: false,  // do not transform the data into query string, otherwise it'll cause 400 error.
      contentType: 'application/json',
      crossDomain: true,
      xhrFields: { withCredentials: true }
    })
    .done(function success(data, status, respObj) {
      console.info('Login successful status =', status);
    })
    .fail(function fail(err) {
      console.error('Error authenticating to Fusion, error =', err);
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
    return fusionUrl + API_APOLLO + path;
  }

  function getFromCatalogAPI(fusionUrl, path) {
    var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion" + path);
    return $.ajax({
      method: 'GET',
      url: callUrl,
      xhrFields: { withCredentials: true }
    });
  }

  function postToCatalogAPI(fusionUrl, path, postData) {
    var callUrl = buildFusionCallUrl(fusionUrl, path);
    return $.ajax({
      method: 'POST',
      url: callUrl,
      data: JSON.stringify(postData),
      processData: false,
      contentType: 'application/json',
      crossDomain: true,
      xhrFields: { withCredentials: true }
    })
    .then(function success(data) {
      return data;
    }, function failure(err) {
      // if err === 401 Unauthorized, try to perform auth
      if (err.status === 401) {
        console.warn('Unauthorized request, trying to login with the input username and password...');
        return doAuth(fusionUrl, tableau.username, tableau.password)
          .then(function() {
            // Resend the POST request
            console.info('Resending the POST request...');
            return $.ajax({
              method: 'POST',
              url: callUrl,
              data: JSON.stringify(postData),
              processData: false,
              contentType: 'application/json',
              crossDomain: true,
              xhrFields: { withCredentials: true }
            })
            .then(function success(data) {
              console.info('Resent the POST request successfully');
              return data;
            }, function failure(err) {
              console.error('Error resending the POST request, error =', err);
              return err;
            });
          })
          .then(function success(data) {  // This will force the chained calls above to finish before returning data.
            return data;
          });
      } else {
        console.error('Error sending the POST request, error =', err);
        tableau.abortWithError("Failed to send the POST request due to: ("+err.status+") "+err.responseJSON.details);
        return err;
      }
    });
  }

  function describeTable(fusionUrl, conn) {
    var tableName = conn.id;
    var tableSchemaPath = "/assets/" + tableName + "/schema";
    
    return getFromCatalogAPI(fusionUrl, tableSchemaPath)
      .then(function(obj) {
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
        console.error('Error getting schema data from Catalog API, err =', err);
      });
  }

  function encodeFieldId(fieldName) {
    return fieldName.replace(/\./g,"_DOT_");
  }

  function decodeFieldId(fieldId) {
    return fieldId.replace(/_DOT_/g,".");
  }

  // This function is used in getSchema() to load selected Fusion tables.
  function loadTables(fusionUrl, schemaCallback) {
    var sql = { sql:'show tables in default' };
    postToCatalogAPI(fusionUrl, '/catalog/fusion/query', sql)
      .then(function success(data) {
        var tables = [];
        data.forEach(function(t) {
          tables.push({id:t.tableName, alias:t.tableName});
        });
        tables.sort(function(lhs,rhs){return lhs.id.localeCompare(rhs.id);});
        return tables;
      }, function failure(err) {
        console.error('Error loading tables from Catalog API, err =', err);
      })
      .then(function success(data) {
        var schemas = [];
        data.forEach(function(table) {
          schemas.push(describeTable(fusionUrl, table));
        });
        Promise.all(schemas).then(function(data) {
          schemaCallback(data);
        });
      });
  }

  // Load Fusion Tables
  function loadFusionTables(fusionUrl) {
    var sql = { sql:'show tables in default' };
    return postToCatalogAPI(fusionUrl, '/catalog/fusion/query', sql);
  }
})();
