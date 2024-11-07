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

# Setup

In order to use eyeBOLD we need to set up some things in advance.

## Venv

Set up a venv for this project and run the following command to install all requirements.

```
pip install -r requirements.txt
```

Install local pygbif package with 
```
pip install -e my_pygbif
```

Install local kgcpy package using
```
pip install my_kgcpy
```
Note: This insalls two packages which differ from their original packages: kgcpy, pygbif.
      It is mandatory to install the adapted kgcpy version.
      The adapted pygbif version introduces https and skips e-mail notifications. If it is not installed GBIF_EMAIL must be set as environmental variable.

## Environmental Variables

Make sure to export the following environmental variables:

GBIF_USER="<your_username>"

GBIF_PWD="<your_password>"

(GBIF_EMAIL="<your_email>")


## Download and Unpack BOLD

Visit the [BOLD-Website](https://bench.boldsystems.org/index.php/datapackages/Latest) and download the data package.
The download is only open to registered users.

Unpack the data package. Both files contained are needed to build the database.

- BOLD_Public.*.tsv
- BOLD_Public.*.datapackage.json

Both files serve as input for our tool. The TSV-File provides the data, the JSON-File describes the dataformat and is parsed.


## Install Rust Compiler

Visit the [Rust-Website](https://www.rust-lang.org/tools/install) and install the compiler.

## Build RaxTax

With the Rust compiler installed, we can build RaxTax.
We provide a [script](/setup_tools/build_raxtax.py) to do this. The executable is automatically copied to the dictionary where it's needed.

```
python build_raxtax.py
```

Note: This process is only tested on Windows. **Known Bug: As RaxTax changed, the checksums for the testfiles are different. The script will say that the process failed.**

## Building the Database
As things are set up, we are ready to build the enhanced BOLD-Database.

Run the following command with your own arguments to build the database from scratch.
**Warining: The process takes multiple days for the complete BOLD-Database.**
This example creates the database my_db.db with all COI-5P sequences, my_loc_db.db containing location information.
The downloaded files from above serve as input.

```
python main.py my_db.db my_loc_db.db COI-5P build BOLD_PUBLIC.*.tsv BOLD_PUBLIC.*.datapackage.json
```


# Usage

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
