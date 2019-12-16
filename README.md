**Overview**

This program scrapes US stock market information, computes factor exposures, detects outliers and standardize factors appropriately. The next step is to write the code for computing cross-sectional regressions. The final steps will be to forecast alpha and risk, in order to give as inputs to an optimization model that outputs the portfolio weights.

**Code structure**

File _main_ is the program to run, as it will call everything else.

The json file _config_ needs to be created. It controls the process pipeline. It also contains all the necessary passwords, as the database password and API keys for scraping. This file was unversioned for obvious reasons. File _config_template_ was provided as a guide.

The class _ManagerSQL_ allows to handle information on a PostgreSQL local database. Only minor changes need to be made to make it work with MySQL. All the necessary queries and data to set up the database are provided in the folders Queries and Data.

The class _WebScraper_ scrapes Tiingo for price and volume information, and scrapes SEC for fundamental information. The core functionality to scrape IEX is also provided, but the database setup expects only Tiingo information. The computation is done in parallel (per ticker) to gain important time savings.

The class _ProcessData_ runs two separate processes. The first process runs in parallel for every ticker and calculates factor exposures (only the most basic ones for now). This includes linking daily price information with periodic, unfrequent, often redundant, often missing, accounting reports. The second process runs in parallel for every date and detects outliers using robust stats and accounting for possible skewness in the data, and scales the data considering appropriate weights.

