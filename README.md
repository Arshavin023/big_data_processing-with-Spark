# Processing of bank transactions from third-party channels
## Overview
This repository contains a data pipeline for processing bank transactions from third-party channels uploaded daily on a network shared drive.

# Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Execution Flow](#execution-flow)


## Introduction <a name="introduction"></a>
The report generation process consist of Python scripts meant for processing bank transactions from third-party channels. These transactions on often uploaded periodically to network shared folder. 
Windows Task Scheduler was utilized to execute the Python scripts daily by 10:00:00am.

## Prerequisites <a name="prerequisites"></a>
Before running this report generation process, the following prerequisite must be meant.

- Installed Python 3.x
- PostgreSQL database with appropriate partitioned tables
- psycopg2 library (pip install psycopg2)
- pandas library (pip install pandas)
- spark library (pip install spark)
- sqlalchemy (pip install sqlalchemy)
- Virtual environment in directory with Python Script

## Installation <a name="installation"></a>

Clone the repository to your local machine:

``` 
git clone https://github.com/Arshavin023/big_data_processing-with-Spark.git
```

Install the required Python packages:

```
pip install -r requirements.txt
```


## Execution Flow
- Set-up Windows Task Scheduler to run Python scripts daily