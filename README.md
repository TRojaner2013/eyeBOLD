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

# Usage

# A word about GBIF

During the setup process, we need to authenticate against the GBIF API. It's important to be aware that pygbif, the library we use to interact with GBIF, utilizes HTTP for communication. This means the data transfer might not be encrypted by default.

While we don't store passwords and usernames in plain text files or directly within the script, the credentials are stored in environment variables. These variables can be potentially accessed by other processes or even malicious actors if not secured properly. Additionally, the credentials are passed to pygbif, which adds another layer of potential exposure.

To mitigate these risks, we strongly recommend using a dedicated GBIF account specifically for this script. Here are some best practices:

* **Create a Shared/Public Account:** If you don't require access to sensitive data, consider using a shared or public GBIF account that doesn't store any personal information or relevant datasets.
* **Use Dummy Credentials:** Alternatively, if a dedicated account is needed but sensitive data isn't involved, create a dummy account with non-functional credentials solely for authentication purposes.

# Updating Submodules
Updating submodules to new commits/versions implies that proper actions are taken that changes are reflected in this project.
This includes creating new binaries for RaxTax and updating the packages with manuall installation in pip.
