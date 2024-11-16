""" Module implementing helper functions for our database

    ToDo: Rename this file to lowercase.
"""

from enum import Enum
import logging
from typing import List, Tuple

import sqlite3

logger = logging.getLogger(__name__)

class BitIndex(Enum):
    """ Class that models bitvector implemented in database as checks column """
    # Notes:
    # We need to flag both: checked and failed names.
    # This is due to the fact that we need to know if a name was checked
    # and if its valid. Otherwise we would mark them as not reviewed.

    SELECTED =               0 # Selected for further processing
    NAME_CHECKED =           1 # Name checked against GIBF
    DUPLICATE =              2 # Sequence is a dubliate or subsequence
    FAILED_LENGTH =          3 # Sequence failed length check
    HYBRID =                 4 # Hybrid status according to regex check
    INCL_KINGDOM =           5 # Verified kingdom with GBIF
    INCL_PHYLUM =            6 # Verified phylum with GBIF
    INCL_CLASS =             7 # Verified class with GBIF
    INCL_ORDER =             8 # Verified order with GBIF
    INCL_FAMILY =            9 # Verified family with GBIF
    INCL_SUBFAMILY =        10 # Verified subfamily with GBIF (NOT IMPLEMENTED)
    INCL_TRIBE =            11 # Verified tribe with GBIF (NOT IMPLEMENTED)
    INCL_GENUS =            12 # Verified genus with GBIF
    INCL_SPECIES =          13 # Verified species with GBIF
    INCL_SUBSPECIES =       14 # Verified subspecies with GBIF (NOT IMPLEMENTED)
    BAD_CLASSIFICATION =    15 # Misclassification according to RaxTax
    NAME_FAILED =           16 # GBIF Name check provided no match
    LOC_CHECKED =           17 # Location checked against GBIF
    LOC_PASSED =            18 # Location is uncertain
    LOC_EMPTY =             19 # Unable to check location - no occurrence data

    @classmethod
    def get_update_clear_mask(cls) -> int:
        """ Returns a mask that can be used to clear bits during update process
        
            Note:
                As of now we just reset all bits except for the location related ones.
                All other bits are resetted during the update process anyways.

            Returns:
                int: Mask used to clear bits in update process
        """

        # Set the bits to one that shall be kept
        # and apply the mask by changes = changes & mask
        mask = 0
        mask |= (1 << cls.LOC_CHECKED.value)
        mask |= (1 << cls.LOC_EMPTY.value)
        mask |= (1 << cls.LOC_PASSED.value)
        return mask

    @classmethod
    def get_golden(cls) -> Tuple[int, int]:
        """ Returns read mask and golden flags 

            Note:
                This is a mask that needs to be set/unset in order for an record
                to becomoe selected.

            Returns:
                Tuple[int, int]: Tuple containing read_mask and golden_mask
        """
        # Set the bits to the values they should have
        golden_mask = 0
        golden_mask |= (1 << cls.NAME_CHECKED.value)
        golden_mask |= (0 << cls.NAME_FAILED.value)
        golden_mask |= (0 << cls.DUPLICATE.value)
        golden_mask |= (0 << cls.FAILED_LENGTH.value)
        golden_mask |= (0 << cls.BAD_CLASSIFICATION.value)

        # Set the bits you want to read to 1
        read_mask = 0
        read_mask |= (1 << cls.NAME_CHECKED.value)
        read_mask |= (1 << cls.NAME_FAILED.value)
        read_mask |= (1 << cls.DUPLICATE.value)
        read_mask |= (1 << cls.FAILED_LENGTH.value)
        read_mask |= (1 << cls.BAD_CLASSIFICATION.value)

        return read_mask, golden_mask

    @classmethod
    def from_string(cls, name: str) -> "BitIndex":
        """ Returns bitindex based on input string

            Note: Not all bit indexes are supported.

            Args:
                - name (str): Name of the bitindex

            Returns:
                - BitIndex: Bitindex based on input string
        """

        san_str = name.lower().strip()
        ret_dict = {'kingdom': cls.INCL_KINGDOM,
                    'phylum': cls.INCL_PHYLUM,
                    'class': cls.INCL_CLASS,
                    'order': cls.INCL_ORDER,
                    'family': cls.INCL_FAMILY,
                    'subfamily': cls.INCL_SUBFAMILY,
                    'tribe': cls.INCL_TRIBE,
                    'genus': cls.INCL_GENUS,
                    'species': cls.INCL_SPECIES,
                    'subspecies': cls.INCL_SUBSPECIES
        }

        res = ret_dict.get(san_str, None)

        if res is None:
            raise ValueError(f"Undefined string {name} provided.")

        return res

# Deprecated
# @dataclass
# class InfoStruct:
#     """Implements a bitvector for information stored in database"""
#     bitvector: int

#     # Data extracted from bitvector
#     selected: bool = field(init=False)
#     dublicate: bool = field(init=False)
#     hybrid: bool = field(init=False)
#     name_checked: bool = field(init=False)

#     incl_kingdom: bool = field(init=False)
#     incl_phylum: bool = field(init=False)
#     incl_class: bool = field(init=False)
#     incl_order: bool = field(init=False)
#     incl_family: bool = field(init=False)
#     incl_subfamily: bool = field(init=False)
#     incl_tribe: bool = field(init=False)
#     incl_genus: bool = field(init=False)
#     incl_species: bool = field(init=False)
#     incl_subspecies: bool = field(init=False)

#     def __post_init__(self):
#         # Assign values from bitvector to instance attributes.
#         bit_mask = 1
#         self.selected = bool(self.bitvector & bit_mask)
#         self.duplicate = bool(self.bitvector & (bit_mask << 1))
#         self.hybrid = bool(self.bitvector & (bit_mask << 2))
#         self.name_checked = bool(self.bitvector & (bit_mask << 3))

#         self.incl_kingdom = bool(self.bitvector & (bit_mask << 4))
#         self.incl_phylum = bool(self.bitvector & (bit_mask << 5))
#         self.incl_class = bool(self.bitvector & (bit_mask << 6))
#         self.incl_order = bool(self.bitvector & (bit_mask << 7))
#         self.incl_family = bool(self.bitvector & (bit_mask << 8))
#         self.incl_subfamily = bool(self.bitvector & (bit_mask << 9))
#         self.incl_tribe = bool(self.bitvector & (bit_mask << 10))
#         self.incl_genus = bool(self.bitvector & (bit_mask << 11))
#         self.incl_species = bool(self.bitvector & (bit_mask << 12))
#         self.incl_subspecies = bool(self.bitvector & (bit_mask << 13))


#     def update_bitvector(self) -> None:
#         """ Updated bitvector of dataclass"""
#         bitvector = 0
#         bit_mask = 1

#         if self.selected:
#             bitvector |= bit_mask
#         if self.dublicate:
#             bitvector |= bit_mask << 1
#         if self.hybrid:
#             bitvector |= bit_mask << 2
#         if self.name_checked:
#             bitvector |= bit_mask << 3

#         if self.incl_kingdom:
#             bitvector |= bit_mask << 4
#         if self.incl_phylum:
#             bitvector |= bit_mask << 5
#         if self.incl_class:
#             bitvector |= bit_mask << 6
#         if self.incl_order:
#             bitvector |= bit_mask << 7
#         if self.incl_family:
#             bitvector |= bit_mask << 8
#         if self.incl_subfamily:
#             bitvector |= bit_mask << 9
#         if self.incl_tribe:
#             bitvector |= bit_mask << 10
#         if self.incl_genus:
#             bitvector |= bit_mask << 11
#         if self.incl_species:
#             bitvector |= bit_mask << 12
#         if self.incl_subspecies:
#             bitvector |= bit_mask << 13

#         self.bitvector = bitvector

class ChecksManager:
    """ Class that manages the checks column in the database 

        Note:
            This class can be used to set and clear bits in the checks column
            of the database, using the BitIndex enum.
    """

    def __init__(self, db_handle: sqlite3.Connection) -> "ChecksManager":
        """ Returns instance of the ChecksManager class

            Args:
                - db_handle (sqlite3.Connection): Database connection
        """
        self._db_handle = db_handle

    @staticmethod
    def generate_mask(bit_indices: List[BitIndex]) -> int:
        """ Generates a read mask based on the input list

            Args:
                - bit_indices (List[BitIndex]): List of BitIndex enums

            Returns:
                - int: Mask generated based on input list
        """
        mask = 0
        for bit_index in bit_indices:
            mask |= (1 << bit_index.value)
        return mask

    def set_bit(self, specimen_ids: List[int], bit_index: BitIndex) -> None:
        """ Sets a single bit in the checks column of the database

            Args:
                - specimen_ids (List[int]): List of specimen ids
                - bit_index (BitIndex): BitIndex enum
        """
        cursor = self._db_handle.cursor()
        cmd = (f"UPDATE specimen SET checks = checks | (1 << {bit_index.value}) "
                "WHERE specimenid = ?")
        cursor.executemany(cmd,specimen_ids)
        self._db_handle.commit()

    def clear_bit(self, specimen_id: int, bit_index: BitIndex) -> None:
        """ Clears a single bit in the checks column of the database

            Args:
                - specimen_id (int): Specimen id
                - bit_index (BitIndex): BitIndex enum
        """
        cursor = self._db_handle.cursor()
        cursor.execute("""
            UPDATE specimen
            SET checks = checks & ~(1 << ?)
            WHERE specimenid = ?
        """, (bit_index.value, specimen_id))
        self._db_handle.commit()
