Overview

This program scrapes US stock market information, computes factor exposures, detects outliers, scales the factors appropriately. The next step is to write the code for computing cross-sectional regressions. The final steps will be to forecast alpha and risk, in order to give as inputs to an optimization model.

The code is structured as follows:

File main is the program to run, as it will call everything else.

The json file config needs to be created. It controls the process pipeline. It also contains all the necessary passwords, as the database password and API keys for scraping. This file was unversioned for obvious reasons. File config_template was provided as a guide.

The class ManagerSQL allows to handle information on a PostgreSQL local database. Only minor changes need to be made to make it work with MySQL. All the necessary queries and data to set up the database are provided in the folders Queries and Data.

The class webScraper. Scrapes Tiingo for price and volume information, and scrapes SEC for fundamental information. The core functionality to scrape IEX is also provided, but the database setup expects only Tiingo information. The computation is done in parallel (per ticker) to gain important time savings.

The class processData runs to separate processes. The first process runs in parallel for every ticker and calculates factor exposures (only the most basic ones for now). The second process runt in parallel for every date and detects outliers using robust stats and accounting for possible skewness in the data, and scales the data considering appropriate weights.

