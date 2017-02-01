# Fusion Web Data Connector for Tableau

This repo contains Fusion connector code along with Tableau Web Data Connector (WDC) SDK. This connector is experimental (not officially supported) beta version. Please use github issues to raise any issues to our attention.

Use the Tableau Web Data Connector (WDC) to connect to web data sources from Tableau. This is the repository for the Tableau WDC SDK, which includes developer samples and a simulator to help you create your connectors.

[Visit the project website and documentation here](http://tableau.github.io/webdataconnector/).

# Getting Started

After cloning the repo, cd into the tableau-fusion-wdc directory and do:

        npm install --production
        npm start

You now have a web server and cors proxy running on ports 8888 and 8889 respectively.
For reference see: [Tableau Fusion WDC Docs](http://tableau.github.io/tableau-fusion-wdc/docs/)

Start Fusion 3.0.0 (at least beta5) and run movielens lab (please pull latest changes from [Fusion Spark Bootcamp](https://github.com/lucidworks/fusion-spark-bootcamp). Or try your own data ;-)

*NOTES*: After this step, you should have Fusion SQL Engine running. Otherwise, the Tableau connector will not be able to communicate with Fusion. To start Fusion SQL Engine manually, run this command:

	    $FUSION_HOME/bin/sql  start

Download, install, and launch [Tableau Public](https://public.tableau.com/en-us/s/download).

Click on the Web Data Connector link under Connect and enter: [http://localhost:8888/fusion.html]()

After a few seconds, you should see you tables listed on the left. At this point, you should be able to build worksheets and dashboards

You're welcome to make changes ;-) Just edit `fusion.html` and `fusion.js` in the tableau-fusion-wdc directory.
You can test them in the browser using the Tableau WDC Simulator (you need to download and setup Tableau WDC separately first):

1. Open [http://localhost:8888/Simulator/index.html]()
2. Put `../fusion.html` as the value for the Connector URL

*NOTES*: avoid using str.startsWith("...") as it works in the browser but not when running in Tableau's JS engine.
