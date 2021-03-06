(function() {
  'use strict';
  
  var API_APOLLO = '/api/apollo';
  var DEFAULT_MAX_ROWS = 10000;
  // Selected Fusion tables in the list that will be sent to Tableau along with each table's settings
  var selectedTables = [];

  function Table(name, selected, filters, sample, maxRows) {
    this.name = name;
    this.id = name;     // Required by Tableau getSchema()
    this.alias = name;  // Required by Tableau getSchema()
    this.selected = selected || true;
    this.filters = filters || '';
    this.sample = sample || 0;
    this.maxRows = maxRows || DEFAULT_MAX_ROWS;
  }

  // Create the connector object
  var myConnector = tableau.makeConnector();

  myConnector.init = function(initCallback) {
    var config = getTableauConnectionData();

    if (tableau.phase === tableau.phaseEnum.interactivePhase) {
      if (config.fusionUrl) {
        $('#fusionUrl').val(config.fusionUrl);
      }
      if (config.realmName) {
        $('#fusionRealm').val(config.realmName);
      }
      if (tableau.username) {
        $('#fusionUsername').val(tableau.username);
        // Enable Load Tables button
        $('#loadTablesButton').prop('disabled', false);
      }
      if (tableau.password) {
        $('#fusionPassword').val(tableau.password);
      }
    }

    initCallback();
  };

  myConnector.getSchema = function(schemaCallback) {
    console.info('getSchema()');
    var config = getTableauConnectionData();

    // TODO what to do when there's no selected table?
    if (config.selectedTables.length < 1) {
      schemaCallback([]);
      return;
    }

    var schemas = [];
    config.selectedTables.forEach(function(table) {
      if (table.selected) {
        // console.log('selectedTables table.id =' + table.id);
        schemas.push(describeTable(config.fusionUrl, table));
      }
    });

    Promise.all(schemas).then(function(data) {
      // TODO print data[] as text individually
      // console.log('Promise.all data =', data);
      schemaCallback(data);
    });
  };

  myConnector.getData = function(table, doneCallback) {
    console.info('getData() table =', table);
    var config = getTableauConnectionData();
    var cols = table.tableInfo.columns.map(function(c) {
      // tab gives us back the field ids encoded
      return {"id":decodeFieldId(c.id), "enc":c.id};
    });
    var tableName = table.tableInfo.id;
    var url = '';
    
    config.selectedTables.forEach(function(t) {
      if (tableName === t.id) {
        var fq = '';
        var rows = '';
        if (t.filters) { fq = 'fq=' + t.filters; }
        if (t.sample === 1) {  // If sample value is 1 (or 100%), use param: rows=<numberOfAllRows> instead of sample param.
          rows = 'rows=' + t.maxRows;
        } else if (t.sample > 0) {
          rows = 'sample=' + t.sample;
        } else {
          rows = 'rows=' + DEFAULT_MAX_ROWS;
        }
        url = buildFusionCallUrl(config.fusionUrl, "/catalog/fusion/assets/" + tableName + "/rows?" + rows + '&' + fq);
        console.info("Loading up to " + t.maxRows + " rows for table " + tableName + " with GET to: "+ url);
      }
    });

    oboe({
      url: url,
      method: 'GET',
      withCredentials: true
    })
    .node('![*]', parseRow)
    .done(function(tableData) {
      doneCallback();
    })
    .fail(function(err) {
      if (err.statusCode === 401) {  // Try to do auth and retry getting data
        console.warn('Unauthorized request, trying to login with the input username and password...');
        return doAuth(config.fusionUrl, tableau.username, tableau.password, config.realmName)
          .then(function() {
            console.info('Retrying getting data from Fusion...');
            oboe({
              url: url,
              method: 'GET',
              withCredentials: true
            })
            .node('![*]', parseRow)
            .done(function(tableData) {
              doneCallback();
            })
            .fail(function(err2) {
              tableau.abortWithError("Load data for "+tableName+" failed due to: ("+err2.statusCode+") "+err2.body);
            });
          });
      } else {
        tableau.abortWithError("Load data for "+tableName+" failed due to: ("+err.statusCode+") "+err.body);
      }
    });

    function parseRow(row) {
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
    }
  };

  tableau.registerConnector(myConnector);

  // Called when web page first loads
  $(document).ready(function() {
    $('#showAdvanced').change(showAdvancedChangeCB);
    $('#selectAllTablesCheckbox').change(selectAllTablesCheckboxChangeCB);
    $('#fusionUsername').blur(fusionUsernameBlurCB);

    // Load Tables button
    $('#loadTablesButton').click(function() {
      // Clear table list before loading
      $('#fusionTables').empty();
      // Disable the button until loading is finished to prevent multiple clicks
      $('#loadTablesButton').prop('disabled', true);
      // Clear status labels
      $('#loadTablesSuccess').css('display', 'none');
      $('#loadTablesFail').css('display', 'none');
      // Show progress bar and reset value to 0%
      $('#fusionTablesProgressBar')
        .css('width', '0%')
        .attr('aria-valuenow', 0)
        .text('0%');
      $('.progress').css('display', '');

      var config = {};
      config.fusionUrl = $('#fusionUrl').val().trim();
      // Store credentials in tableau for easy access later
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();
      config.realmName = $('#fusionRealm').val();
      setTableauConnectionData(config);

      // Clear selectedTables
      selectedTables = [];

      loadFusionTables(config.fusionUrl)
        .then(function(data) {
          // console.log('data =', data);
          var countPromises = [];
          var tableTotalNum = data.length;
          var iterNum = 1;  // Cannot use tableIndex from data.forEach()
                            // because the tableIndex order is not guarantee due to promise call.

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
                var selectTableCheckboxId = table.tableName + 'Checkbox';
                var totalRowsColumnId = table.tableName + 'TotalRows';
                var filtersColumnId = table.tableName + 'Filters';
                var sampleColumnId = table.tableName + 'Sample';
                var maxRowsColumnId = table.tableName + 'MaxRows';
                var maxRows = totalRows < 10000 ? totalRows : 10000;

                // Add each table as obj to the array for tracking, the table name is unique.
                var tableObj = new Table(table.tableName);
                selectedTables.push(tableObj);

                // Add a row of metadata to the table list
                $('#fusionTables').append(
                  '<tr>' +
                  '<td><input class="select-table" type="checkbox" checked id="' + selectTableCheckboxId + '"></td>' +
                  '<td>' + table.tableName + '</td>' +
                  '<td>Solr</td>' +
                  '<td id="' + totalRowsColumnId + '">' + totalRows + '</td>' +  // Get total rows
                  '<td>' + '<input type="text" id="' + filtersColumnId + '">' + '</td>' +
                  '<td>' + '<input class="sample" type="number" min="1" max="100" placeholder="10" id="' + sampleColumnId + '">' + '</td>' +
                  '<td class="max-rows" id="' + maxRowsColumnId + '">' + maxRows + '</td>' +
                  '</tr>'
                );
                
                var selectTableCheckboxObj = $('#' + selectTableCheckboxId);
                var totalRowsColumnObj = $('#' + totalRowsColumnId);
                var filtersColumnObj = $('#' + filtersColumnId);
                var sampleColumnObj = $('#' + sampleColumnId);
                var maxRowsColumnObj = $('#' + maxRowsColumnId);
                
                // Attach event listener to Select table checkbox
                selectTableCheckboxObj.change(function() {
                  var isChecked = this.checked;
                  $.each(selectedTables, function(idx, t) {
                    if (t.name === table.tableName) {
                      if (isChecked) { t.selected = true; }
                      else { t.selected = false; }
                    }
                  });
                });

                // Attache event listener to Filters column
                filtersColumnObj.blur(function() {
                  // Update filters in selectedTables[]
                  if (filtersColumnObj.val()) {
                    $.each(selectedTables, function(idx, t) {
                      if (t.name === table.tableName) {
                        t.filters = filtersColumnObj.val();
                      }
                    });
                  } else {
                    $.each(selectedTables, function(idx, t) {
                      if (t.name === table.tableName) {
                        t.filters = '';
                      }
                    });
                  }
                });

                // Attach event listener to Sample column to compute 'Max Rows to Load' value
                sampleColumnObj.blur(function() {
                  if (sampleColumnObj.val()) {
                    // Compute 'Max Rows to Load'
                    var maxValue = Math.round(totalRowsColumnObj.text() * sampleColumnObj.val() / 100);
                    maxRowsColumnObj.text(maxValue);
                    // Update the sample value and maxRows value in selectedTables[]
                    $.each(selectedTables, function(idx, t) {
                      if (t.name === table.tableName) {
                        t.sample = sampleColumnObj.val() / 100;
                        t.maxRows = maxValue;
                      }
                    });
                  } else {  // If sample value is undefined, set max rows to default value.
                    maxRowsColumnObj.text(maxRows);
                    // Update the sample value and maxRows value in selectedTables[]
                    $.each(selectedTables, function(idx, t) {
                      if (t.name === table.tableName) {
                        t.sample = 0;
                        t.maxRows = maxRows;
                      }
                    });
                  }
                });

                // Update fusionTablesProgressBar                
                var progressBarPercent = Math.round(iterNum / tableTotalNum * 100);
                iterNum++;

                $('#fusionTablesProgressBar')
                  .css('width', progressBarPercent+'%')
                  .attr('aria-valuenow', progressBarPercent)
                  .text(progressBarPercent+'%');
              });

            countPromises.push(promise);
          });

          // After finished loading all tables and metadata
          Promise.all(countPromises).then(function() {
            console.info('Finished loading all tables.');            
            $('#loadTablesSuccess').css('display', '');
            $('#submitButton').prop('disabled', false);
            // Update fusionTablesProgressBar
            $('#fusionTablesProgressBar')
              .css('width', '100%')
              .attr('aria-valuenow', 100)
              .text('100%');
            // Set one second delay to hide the progress bar for a nice visual ;)
            // And re-enable the button.
            setTimeout(function() {
              $('.progress').css('display', 'none');
              $('#loadTablesButton').prop('disabled', false);
            }, 1000);
          });
        })
        .fail(function() {
          console.error('Error loading tables.');
          $('#loadTablesFail').css('display', '');
          // Re-enable the button
          $('#loadTablesButton').prop('disabled', false);
        });
    }); // End of Load Tables button

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
      setTableauConnectionData(config);

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

    // Submit button
    $("#submitButton").click(function() {
      var config = {};
      var fusionUrl = $('#fusionUrl').val().trim();
      if (!fusionUrl) {
        fusionUrl = 'http://localhost:8764';
      }
      config.fusionUrl = fusionUrl;
      config.realmName = $('#fusionRealm').val();
      // Store credentials in tableau for easy access later
      tableau.username = $('#fusionUsername').val();
      tableau.password = $('#fusionPassword').val();
      config.changedOn = new Date();  // This ensures Tableau always refreshes the table list from the server on edit
      config.selectedTables = [];  // This array will only store selected tables
      selectedTables.forEach(function(table) {
        if (table.selected) {
          config.selectedTables.push(table);
        }
      });

      setTableauConnectionData(config);
      tableau.connectionName = "Lucidworks Fusion";
      tableau.submit();
    }); // End of Submit button
  }); // End of document.ready()

  // Callback for Show Advanced Options checkbox
  function showAdvancedChangeCB() {
    if (this.checked) {
      $('.advanced-options').css('display', '');
    } else {
      $('.advanced-options').css('display', 'none');
    }
  }

  // Callback for Toggle the all tables checkbox for the table list
  function selectAllTablesCheckboxChangeCB() {
    var isChecked = this.checked;
    if (isChecked) {
      $('.select-table').prop('checked', true);
      // Update all tables to be selected
      $.each(selectedTables, function(idx, t) {
        t.selected = true;
      });
    } else {
      $('.select-table').prop('checked', false);
      // Update all tables to be unselected
      $.each(selectedTables, function(idx, t) {
        t.selected = false;
      });
    }
  }

  // Callback for Fusion Username input
  function fusionUsernameBlurCB() {
    // Enable the Load Tables button when the username is not empty
    if ($(this).val()) {
      $('#loadTablesButton').prop('disabled', false);
    } else {
      $('#loadTablesButton').prop('disabled', true);
    }
  }

  // An on-click function for login to Fusion
  function doAuth(fusionUrl, username, password, realmName) {
    realmName = realmName || 'native';
    // Use Fusion Sessions API to create a session
    fusionUrl += '/api/session?realmName=' + realmName;
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
      console.info('Login successful status =' + status);
    })
    .fail(function fail(err) {
      console.error('Error authenticating to Fusion, error =', err);
    });
  }

  function buildFusionCallUrl(fusionUrl, path) {
    return fusionUrl + API_APOLLO + path;
  }

  function getTableauConnectionData() {
    var config = {};
    if (tableau.connectionData) {
      config = JSON.parse(tableau.connectionData);
    }
    return config;
  }

  function setTableauConnectionData(config) {
    tableau.connectionData = JSON.stringify(config);
  }

  function getFromCatalogAPI(fusionUrl, path) {
    var config = getTableauConnectionData();
    var callUrl = buildFusionCallUrl(fusionUrl, "/catalog/fusion" + path);
    var req = {
      method: 'GET',
      url: callUrl,
      xhrFields: { withCredentials: true }
    };

    return $.ajax(req)
      .then(function success(data) {
        return data;
      }, function failure(err) {
        // if err === 401 Unauthorized, try to perform auth
        if (err.status === 401) {
          console.warn('Unauthorized request, trying to login with the input username and password...');
          return doAuth(fusionUrl, tableau.username, tableau.password, config.realmName)
            .then(function() {
              // Resend the GET request
              console.info('Resending the GET request...');
              return $.ajax(req)
                .then(function success(data) {
                  console.info('Resent the GET request successfully');
                  return data;
                }, function failure(err) {
                  console.error('Error resending the GET request, error =', err);
                  return err;
                });
            })
            .then(function success(data) {  // This will force the chained calls above to finish before returning data.
              return data;
            });
        } else {
          console.error('Error sending the GET request, error =', err);
          tableau.abortWithError("Failed to send the GET request due to: ("+err.status+") "+err.responseJSON.details);
          return err;
        }
      });
  }

  function postToCatalogAPI(fusionUrl, path, postData) {
    var config = getTableauConnectionData();
    var callUrl = buildFusionCallUrl(fusionUrl, path);
    var req = {
      method: 'POST',
      url: callUrl,
      data: JSON.stringify(postData),
      processData: false,
      contentType: 'application/json',
      crossDomain: true,
      xhrFields: { withCredentials: true }
    };

    return $.ajax(req)
      .then(function success(data) {
        return data;
      }, function failure(err) {
        if (err.status === 401) {  // Unauthorized err, try to perform auth
          console.warn('Unauthorized request, trying to login with the input username and password...');
          return doAuth(fusionUrl, tableau.username, tableau.password, config.realmName)
            .then(function() {
              // Resend the POST request
              console.info('Resending the POST request...');
              return $.ajax(req)
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
        } else if (err.status === 0) {  // Problem connecting to Fusion or SQL engine
          console.error('Failed to connect to Fusion server or SQL Engine, error =', err);
          tableau.abortWithError('Failed to connect to Fusion server or SQL Engine. Please check your settings and connection.');
        } else {
          console.error('Error sending the POST request, error =', err);
          tableau.abortWithError("Failed to send the POST request due to: (status = " + err.status + ").");
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

  // Load Fusion Tables
  function loadFusionTables(fusionUrl) {
    var sql = { sql:'show tables in default' };
    return postToCatalogAPI(fusionUrl, '/catalog/fusion/query', sql);
  }
})();
