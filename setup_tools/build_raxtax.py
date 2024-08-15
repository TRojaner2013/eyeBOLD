""" Module to build raxtax """

import subprocess
import shutil
import platform
import os
import hashlib

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_FILE = os.path.join(ROOT_DIR, "raxtax_test.fasta")

# Hash for larger files (not included in git)
#OUT_SHA256 = "ce043c3997fce424b267a442761c7a5f9734bc8c62b7bdefef8a1bf7893e5e20"
#TSV_SHA256 = "294007169fc51f01fbc6377277d591e392ebd7e71db671cf2895167cbbb2c53a"

OUT_SHA256 = "d66cbe2db3d751fea6c07a8c8e6801d26a2e5ba69336ce685a9e02fe4b0b4799"
TSV_SHA256 = "edc5d795a8c903589e49d606b7b789a686db3a4b72372401c49eac1e97aac108"

def _build() -> bool:
    """Builds RaxTax with cargo

        This function calles cargo build with reccomanded flags.
        Cargo must be installed and must be available as command on target machine.
    """
    try:
        subprocess.run(["cargo", "build", "--profile=ultra"],
                       cwd=os.path.join(ROOT_DIR, "..", "raxtax"), check=True)

        return True
    except subprocess.CalledProcessError as exc:
        print(f"Error building RaxTax: {exc}")
        return False

def _copy_bin() -> None:
    """Copies binary file to new location

        This function moves the raxtax binary file to a known location so that
        eyeBOLD is able to use it.
    """
    src_dir = os.path.join(ROOT_DIR, "..", "raxtax", "target", "ultra")
    dst_dir = os.path.join(ROOT_DIR, "..")

    #ToDo: Make sure to use a defined constant as binary name here
    binary_name = "raxtax"

    if platform.system() == "Windows":
        binary_name += ".exe"

    src_file = os.path.join(src_dir, binary_name)
    dst_file = os.path.join(dst_dir, binary_name)

    shutil.copy2(src_file, dst_file)

def _check_bin() -> bool:
    """ Checks raxtax binary 

        This function calls raxtax binarz twice and creates two output files.
        First one creates an .out file, the second run a .tsv files.

        Then we check if the files are identical to reference files using a 
        SHA256 hash.
    """
    try:
        subprocess.run(["raxtax", "-d", TEST_FILE, "-i", TEST_FILE,
                        "--skip-exact-matches", "--redo"], check=True)

        subprocess.run(["raxtax", "-d", TEST_FILE, "-i", TEST_FILE,
                        "--tsv", "--skip-exact-matches", "--redo"], check=True)

        output_files = [(os.path.join(f"{TEST_FILE[:-5]}out", "raxtax.out"), OUT_SHA256),
                        (os.path.join(f"{TEST_FILE[:-5]}out", "raxtax.tsv"), TSV_SHA256)]

        for output_file, expected_hash in output_files:
            with open(output_file, "rb") as f_handle:
                data = f_handle.read()
                sha256 = hashlib.sha256(data).hexdigest()
                if sha256 != expected_hash:
                    print((f"Hash for file {output_file} is incorrect.\n"
                           f"Got:\t{sha256}\nExp:\t{expected_hash}"))
                    return False

        return True

    except subprocess.CalledProcessError as exc:
        print(f"Error checking binary: {exc}")
        return False

def _clean() -> None:
    """Clean build artrfacts and output files.

        This cleans all building artefacts using cargo clean and removes output
        files created by calling raxtex.
    """
    try:
        subprocess.run(["cargo", "clean"],
                       cwd=os.path.join(ROOT_DIR, "..", "raxtax"), check=True)

        output_dir = f"{TEST_FILE[:-5]}out"
        shutil.rmtree(output_dir)

    except subprocess.CalledProcessError as exc:
        print(f"Failed to clean up: {exc}")

if __name__ == "__main__":

    print("Building RAxTax...")

    if not _build():
        print("Failed to build RAxTax.")
        raise SystemExit

    _copy_bin()

    if not _check_bin():
        print("Binary reurned different results than expected.")
        raise SystemExit

    _clean()
    print("RAxTax sucessfully build and verified.")
