# eyeBOLD
 BOLD, effortlessly enhanced.

# Requirements
- BOLD Account
- GBIF Account
- Set environmental variables for GBIF or include in .gbif file
- SKIP EMAIL Option
- Install rust compiler (cargo)
- Run setup_tools script to build raxtax
- DISKSPACE!! (We are talking about a looooot like idk 200 - 300 GB)
- Time (

# Usage

# Building the database

The first step is to build the database with data provided by BOLD and GBIF.
Figure x illustrates the process.
Some parts are implemented in a sequential order, other parts are executed in parallel.

Espacially downloading the geo data from GBIF is time consuming. The process will take around one or two days depending on your internet connetion.

Use command line to build the curated database:

1. python main.py db loc_db marker build tsv datapackage
2. python main.py db loc_db marker review -- This is done to fix all errrors on first build process
3. Use database with queries e.g. python main db loc_db marker query "SELECT * FROM specimen where include = 1;"

# A word about GBIF

During the setup process, we need to authenticate against the GBIF API. It's important to be aware that pygbif, the library we use to interact with GBIF, utilizes HTTP for communication. This means the data transfer might not be encrypted by default.

While we don't store passwords and usernames in plain text files or directly within the script, the credentials are stored in environment variables. These variables can be potentially accessed by other processes or even malicious actors if not secured properly. Additionally, the credentials are passed to pygbif, which adds another layer of potential exposure.

To mitigate these risks, we strongly recommend using a dedicated GBIF account specifically for this script. Here are some best practices:

* **Create a Shared/Public Account:** If you don't require access to sensitive data, consider using a shared or public GBIF account that doesn't store any personal information or relevant datasets.
* **Use Dummy Credentials:** Alternatively, if a dedicated account is needed but sensitive data isn't involved, create a dummy account with non-functional credentials solely for authentication purposes.

# Updating Submodules
Updating submodules to new commits/versions implies that proper actions are taken that changes are reflected in this project.
This includes creating new binaries for RaxTax and updating the packages with manuall installation in pip.
