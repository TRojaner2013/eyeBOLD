# eyeBOLD

Welcome to the eyeBOLD project - BOLD, effortlessly enhanced.
Our project aims at providing an automated way to curate the BOLD database.
Feel free to test and share your results and experiences with us.

This project is a result of my thesis and certainly not yet perfect.
Therefore, feel free to help with new features, enhancements or just leave feedback :)

# Requirements

In order to work with eyeBOLD you will need the following things:

- BOLD Account
- GBIF Account
- Good and stable internet connection
- Diskspace (We recommand at least 200 GB, min. should be aroung roughly 120 GB)
- A good book or other stuff to do while running the curation process. (Takes several days)

When we provide some time estimations, these are taken from a machine with 32 GB Ram and a Intel Xeon W-10885M CPU with high-speed fiber internet.

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
python -m setup_toos.build_raxtax
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

## Building the Location Database

Now that the entries are curated, we can assign score values to the locations of each specimen with coordinates.
To do so, we need to build the location database with the following command:

```
python main.py my_db.db my_loc_db.db COI-5P build-location-db -s 1500    
```

The -s flag indicates for how many species location data are downloaded in parallel.
Note, that there is a hard coded maximum at GBIFs side.
In order to save disk space, we strongly recommand to keep this at max. 1500 depending on you disk size.
The smaller the value, the smaller the download will be.
Keep in mind that we need to downloas a lot of data.
Being limited by GBIFs response time, this process most likely takes over 4d to complete.
**We strongly encurage you to use the database provided by us in a future release**

# Usage

Now that we build the database and got the locations, we can start to use our own queries to export data.
Note that currently no FASTA export is implemented, use the RAXTAX export instead, as it is a form of FASTA.
However, before we talk about the export, let us take a glance at the _update_ and _review_ command.

## Review

Sometimes we can ran into problems with name lookups in GBIF.
When eyeBOLD fails to lookup a name multiple times, an error is shown during the build process.

To solve the problem simply run:

```
python main.py my_db.db my_loc_db.db COI-5P review
```

Repeat the command untill no more errors are displayed.

## Update Procedure

When you want to update your curated database with new data, simply run the update procedure:

```
python main.py my_db.db my_loc_db.db COI-5P update NEW_BOLD_PUBLIC.*.tsv NEW_BOLD_PUBLIC.*.datapackage.json

```

This process will also take some time to finish. Afterwards, we need to run the _build-loc-db_ command again.


## Export all Selected Entries

If you want to export all selected entries from BOLD you can use the _export_ command.
Using our example from above, we could simply use the following command:

```
python main.py my_db.db my_loc_db.db COI-5P export TSV output.tsv
```
Valid output formats are TSV, CSV, RAXTAX and FASTA.

## Exporting own Datasets

When you want to create you own dataset to export, you will need to create costum SQL-queries.
These can then be evaluated using the query command.
In our example we will ask for all entries where the gbif_key equals 1546413 -- this queries for all specimen identified only to genus level where the genus is _Megaselia Rondani_.
The gbif_key can be found at the end of a GBIF URL e.g for our [example](https://www.gbif.org/species/1546413).

Given the gbif_key, we can then simply create an SQL query:
```
Select * from specimen where gbif_key = 1546413;
```

Which we can use with eyeBOLD in the following manner:

```
python main.py my_db.db my_loc_db.db COI-5P query "Select * from specimen where gbif_key = 1546413;" -f TSV -o output.tsv

```

Note that we here selected the format (-f) and output (-o) option in order to store the result on disk.
If you want to store the result, you must provide both options.
If neither are provided, results are printed on screen.
Currently this command only supports TSV and CSV outputs.


## Database Layout

You might wonder, what kind of querys you can perfom.
Feel free to use the following tables as reference.

The table *processing_input* includes all data from BOLD as provided.
Depending on the data provided by bold, the layout might look different.
Thus, we will not provide a detailed description here.

The table *specimen* includes all curated data.

| Column Name | Datatype | Description |
| ----------- | -------- | ----------- |
| specimenid | INTEGER | Primary Key, Specimenid from BOLD |
| nuc_raw | TEXT | Raw sequence from BOLD |
| nuc_san | TEXT | Curated sequence data |
| geo_info | FLOAT | Location score |
| hash | VARCHAR(64) | Hash value for record |
| last_updated | DATE | Last update of record |
| review | BOOLEAN | Record must be reviewed |
| processing_info | JSON | GBIF output |
| include | BOOLEAN | Record selected/passed all checks |
| gbif_key | INTEGER | Accepted taxon key in GBIF |
| taxon_rank | VARCHAR(255) | Identification rank in BOLD |
| taxon_kingdom | VARCHAR(255) | Taxon Kingdom |
| taxon_kingdom | VARCHAR(255) | Taxon Phylum |
| taxon_kingdom | VARCHAR(255) | Taxon Class |
| taxon_kingdom | VARCHAR(255) | Taxon Order |
| taxon_kingdom | VARCHAR(255) | Taxon Family |
| taxon_kingdom | VARCHAR(255) | Taxon Subfamily |
| taxon_kingdom | VARCHAR(255) | Taxon Tribe |
| taxon_kingdom | VARCHAR(255) | Taxon Genus |
| taxon_kingdom | VARCHAR(255) | Taxon Species |
| taxon_kingdom | VARCHAR(255) | Taxon Subspecies |
| identification_rank | VARCHAR(255) | GBIF verified rank |
| checks | INTEGER | Bitvector with checks |
| contry_iso  | VARCHAR(3) | ISO Code where specimen was found |
| kg_zone | VARCHAR(5) | KG-climate zone where specimen was found |

Given this table you can create queries.
Note that gbif_key is indexed and thus provides faster results.

All entires must have a specimenid as well as a nuc_raw.
Other columns can be empty. 
Contry_iso and kg_zone are only available, when the original record included a contry and coordinates.
The taxa names are from BOLD and curated, untill the rank indicated in checks.

Checks is a bitvector with the follwoing bits:
| Bit | Description |
| --- | ----------- |
| 0 | Selected for further processing |
| 1 | Name checked against GIBF |
| 2 | Sequence is a dupliate or subsequence |
| 3 | Sequence failed length check |
| 4 | Hybrid according to regex check |
| 5 | Verified kingdom with GBIF |
| 6 | Verified phylum with GBIF |
| 7 | Verified class with GBIF |
| 8 | Verified order with GBIF |
| 9 | Verified family with GBIF |
| 10 | Verified subfamily with GBIF (NOT IMPLEMENTED) |
| 11 | Verified tribe with GBIF (NOT IMPLEMENTED) |
| 12 | Verified genus with GBIF |
| 13 | Verified species with GBIF |
| 14 | Verified subspecies with GBIF |
| 15 | Misclassification according to RaxTax |
| 16 | GBIF Name check provided no match |
| 17 | Location checked against GBIF |
| 18 | Location is uncertain |
| 19 | Unable to check location - no occurrence data |


# A word about GBIF

During the setup process, we need to authenticate against the GBIF API. It's important to be aware that pygbif, the library we use to interact with GBIF, utilizes HTTP for communication. This means the data transfer might not be encrypted by default.

While we don't store passwords and usernames in plain text files or directly within the script, the credentials are stored in environment variables. These variables can be potentially accessed by other processes or even malicious actors if not secured properly. Additionally, the credentials are passed to pygbif, which adds another layer of potential exposure.

To mitigate these risks, we strongly recommend using a dedicated GBIF account specifically for this script. Here are some best practices:

* **Use a Shared/Public Account:** If you don't require access to sensitive data, consider using a shared or public GBIF account that doesn't store any personal information or relevant datasets.
* **Use Dummy Credentials:** Alternatively, if a dedicated account is needed but sensitive data isn't involved, create a dummy account with non-functional credentials solely for authentication purposes.

# Updating Submodules
The commits of the submodules are checked to be working. Feel free to try newer versions at your own risk.
