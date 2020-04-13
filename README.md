# Wage Trust Data Analysis Workflow Demo

### What is it?
This repo provides a demo for standard data analysis workflow. It includes the following two containers:
1. A PostgreSQL database
2. Build-in modules for serving RDA(Awards) testings as app.

### Project organisation documents
- Database relationship diagram:
    - https://app.dbdesigner.net/designer/schema/300586
- Workflow design:
    - https://docs.google.com/presentation/d/1bBYGe2z8csv3KGRunilwghvsaZma15y7KHf29lajY_k/edit?usp=sharing


### How do I use it?
#### Preparation
0. If you have not already set up your docker environment to use the PwC proxy, please do so now. If you have not made any edits to `~/.docker/config.json`, simply do:
```
$ mv pwc_config.json ~/.docker/config.json
```
or if you have local edits you wish to preserve, simply append the proxy information to your `config.json` file. For windows users, I am not sure where the file is located as I do not have a windows machine to experiment with.
1. Collect all relevant data and save them to the google drive under '/Data/raw'. 
    - https://drive.google.com/open?id=1T_Ul9pf7HPI1WbAssyozrU6vxnPRfesb
2. The data under '/Data/support/' is the support data such as public holidays. Please keep it as it is.
3. Make your machine ready. Please make sure you already have:
    - PwC github account
    - git installed
    - docker installed
    - ensure you have the Data folder structure:
```
+--workflow_demo
  +--Data
    +--output
    +--raw
    +--support
```
    

#### Step-by-Step
1.Clone this repo to your local machine
- Use the terminal to move into where you want to save the repo locally.
```
$ git clone {ssh from github repo}
```

2.Copy '/Data' from the google drive to the /Data folder locally.

3.Build the containers via:
```
$ ./docker-build.sh
```

4.Run them via

```
$ docker-compose up -d
```
5.Dump all the data in '/Data/support/' and '/Data/raw' to the postgresql database. Note that, the table with 'spt_' prefix is the support data, while the raw data is initialed with 'raw_'.
- Two python scripts: dump_raw_data.py & dump_support_data.py are provided as reference. You could run them to dump the data.
- Or choose your own way to dump the data to your local database.
- It is suggested that you use the prefix 'cln_' for the standard outputs, which is used to make sure all outputs of data cleansing steps follow the predefined format. You don't need to share those tables since the team member will have the same data after running your code.
- cd into the data_preperation folder and then
```
python3 dump_raw_data.py
```

6.If you prefer using sql to process the data, you could access the database via:
- Host: 127.0.0.1, port: 9876
- Database: postgres
- Username: postgres
- Password: docker
- Open dbeaver and check within the schema that this has been created

7.Open juypter lab by using the terminal to cd to the data_preperation folder withimn the repo.
```
$ jupyter lab
```

8.Open "data_preperation.ipynb" file and run step by step to generate results which save back into the database:

9.Connect tableau to your database container.
- Open tableau and select connect to a PostgreSQL server with the following details:
    - Server: 127.0.0.1
    - Port: 9876
    - Database: postgres
    - Authentication: Username and Password
    - Username: postgres
    - Password: docker
- Then select "Sign In"

