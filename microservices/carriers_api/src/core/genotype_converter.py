from abc import ABC, abstractmethod
import pandas as pd
from typing import Any


class GenotypeConverter(ABC):
    @abstractmethod
    def convert(self, genotype: Any, snp_allele: str) -> str:
        """Convert a genotype value to a string representation"""
        pass


class StandardGenotypeConverter(GenotypeConverter):
    def convert(self, genotype: Any, snp_allele: str) -> str:
        if pd.isna(genotype):
            return './.'
        genotype = int(genotype)
        if genotype == 2:
            return "WT/WT"
        elif genotype == 1:
            return f"WT/{snp_allele}"
        elif genotype == 0:
            return f"{snp_allele}/{snp_allele}"
        else:
            return ":/:"