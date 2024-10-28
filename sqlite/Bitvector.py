"""Module implementing helper functions for our database"""

import sqlite3
from enum import Enum
from typing import List, Tuple
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

class BitIndex(Enum):
    # Notes:
    # We need to flag both: checked and failed names.
    # This is due to the fact that we need to know if a name was checked
    # and if its valid. Otherwise we would mark them as not reviewed.

    SELECTED =           0 # Selected for further processing
    NAME_CHECKED =       1 # Name checked against GIBF
    DUPLICATE =          2 # Sequence is a dubliate or subsequence
    FAILED_LENGTH =      3 # Sequence failed length check
    HYBRID =             4 # Hybrid status according to regex check
    INCL_KINGDOM =       5 # Verified kingdom with GBIF
    INCL_PHYLUM =        6 # Verified phylum with GBIF
    INCL_CLASS =         7  # Verified class with GBIF
    INCL_ORDER =         8 # Verified order with GBIF
    INCL_FAMILY =        9 # Verified family with GBIF
    INCL_SUBFAMILY =    10 # Verified subfamily with GBIF (NOT IMPLEMENTED)
    INCL_TRIBE =        11 # Verified tribe with GBIF (NOT IMPLEMENTED)
    INCL_GENUS =        12 # Verified genus with GBIF
    INCL_SPECIES =      13 # Verified species with GBIF
    INCL_SUBSPECIES =   14 # Verified subspecies with GBIF (NOT IMPLEMENTED)
    BAD_CLASSIFICATION = 15 # Misclassification according to RaxTax
    NAME_FAILED =        16 # GBIF Name check provided no match

    @classmethod
    def get_golden(cls) -> Tuple[int, int]:
        """ Returns golden flags 

            This is a mask that needs to be set/unset in order for an record
            to becomoe selected.
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
        """Returns bitindec based on input string """

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

@dataclass
class InfoStruct:
    """Implements a bitvector for information stored in database"""
    bitvector: int

    # Data extracted from bitvector
    selected: bool = field(init=False)
    dublicate: bool = field(init=False)
    hybrid: bool = field(init=False)
    name_checked: bool = field(init=False)

    incl_kingdom: bool = field(init=False)
    incl_phylum: bool = field(init=False)
    incl_class: bool = field(init=False)
    incl_order: bool = field(init=False)
    incl_family: bool = field(init=False)
    incl_subfamily: bool = field(init=False)
    incl_tribe: bool = field(init=False)
    incl_genus: bool = field(init=False)
    incl_species: bool = field(init=False)
    incl_subspecies: bool = field(init=False)

    def __post_init__(self):
        # Assign values from bitvector to instance attributes.
        bit_mask = 1
        self.selected = bool(self.bitvector & bit_mask)
        self.duplicate = bool(self.bitvector & (bit_mask << 1))
        self.hybrid = bool(self.bitvector & (bit_mask << 2))
        self.name_checked = bool(self.bitvector & (bit_mask << 3))

        self.incl_kingdom = bool(self.bitvector & (bit_mask << 4))
        self.incl_phylum = bool(self.bitvector & (bit_mask << 5))
        self.incl_class = bool(self.bitvector & (bit_mask << 6))
        self.incl_order = bool(self.bitvector & (bit_mask << 7))
        self.incl_family = bool(self.bitvector & (bit_mask << 8))
        self.incl_subfamily = bool(self.bitvector & (bit_mask << 9))
        self.incl_tribe = bool(self.bitvector & (bit_mask << 10))
        self.incl_genus = bool(self.bitvector & (bit_mask << 11))
        self.incl_species = bool(self.bitvector & (bit_mask << 12))
        self.incl_subspecies = bool(self.bitvector & (bit_mask << 13))


    def update_bitvector(self) -> None:
        """ Updated bitvector of dataclass"""
        bitvector = 0
        bit_mask = 1

        if self.selected:
            bitvector |= bit_mask
        if self.dublicate:
            bitvector |= bit_mask << 1
        if self.hybrid:
            bitvector |= bit_mask << 2
        if self.name_checked:
            bitvector |= bit_mask << 3

        if self.incl_kingdom:
            bitvector |= bit_mask << 4
        if self.incl_phylum:
            bitvector |= bit_mask << 5
        if self.incl_class:
            bitvector |= bit_mask << 6
        if self.incl_order:
            bitvector |= bit_mask << 7
        if self.incl_family:
            bitvector |= bit_mask << 8
        if self.incl_subfamily:
            bitvector |= bit_mask << 9
        if self.incl_tribe:
            bitvector |= bit_mask << 10
        if self.incl_genus:
            bitvector |= bit_mask << 11
        if self.incl_species:
            bitvector |= bit_mask << 12
        if self.incl_subspecies:
            bitvector |= bit_mask << 13

        self.bitvector = bitvector

class ChecksManager:

    def __init__(self, db_handle: sqlite3.Connection):

        self._db_handle = db_handle

    @staticmethod
    def generate_mask(bit_indexes: List[BitIndex]) -> int:
        mask = 0
        for bit_index in bit_indexes:
            mask |= (1 << bit_index.value)
        return mask

    def set_bit(self, specimen_ids: List[int], bit_index: BitIndex):
        #ToDo: + Documentation, BitIndexList
        cursor = self._db_handle.cursor()
        cmd = (f"UPDATE specimen SET checks = checks | (1 << {bit_index.value}) "
                "WHERE specimenid = ?")
        cursor.executemany(cmd,specimen_ids)
        self._db_handle.commit()

    def clear_bit(self, specimen_id: int, bit_index: BitIndex):
        cursor = self._db_handle.cursor()
        cursor.execute("""
            UPDATE specimen
            SET checks = checks & ~(1 << ?)
            WHERE specimenid = ?
        """, (bit_index, specimen_id))
        self._db_handle.commit()

    def check_bit(self, specimen_id: int, bit_index: BitIndex):
        cursor = self._db_handle.cursor()
        cursor.execute("""
            SELECT (checks & (1 << ?)) != 0
            FROM specimen
            WHERE specimenid = ?
        """, (bit_index, specimen_id))
        result = cursor.fetchone()
        return result[0] if result else False
