import pandas as pd
from typing import Any, Optional, List, Dict
from src.core.data_repository import DataRepository
from src.core.genotype_converter import GenotypeConverter


class CarrierValidator:
    def __init__(self, data_repo: DataRepository, genotype_converter: GenotypeConverter):
        self.data_repo = data_repo
        self.genotype_converter = genotype_converter
    
    def verify_genotype(self, raw_value: Any, string_value: str, snp_name: str) -> bool:
        """Verify that a genotype string matches its raw value"""
        if pd.isna(raw_value):
            return string_value == './.'
        
        raw_value = int(raw_value)
        if raw_value == 2:
            return string_value == "WT/WT"
        elif raw_value == 1:
            return string_value.startswith("WT/")
        elif raw_value == 0:
            return string_value.count(snp_name.split('_')[1]) == 2
        else:
            return string_value == ":/:"
    
    def validate_carrier_data(self, traw_dir: str, combined_file: str, 
                             snp_info_file: str, samples_to_check: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Validate that combined carrier data matches original traw files.
        
        Args:
            traw_dir: Directory containing traw files
            combined_file: Path to combined carriers file
            snp_info_file: Path to SNP info file
            samples_to_check: List of sample IDs to check (None for all)
            
        Returns:
            List of mismatches found (empty list if all match)
        """
        carriers = self.data_repo.read_csv(combined_file)
        snp_df = self.data_repo.read_csv(snp_info_file)
        
        if samples_to_check is None:
            samples_to_check = carriers['IID'].unique()[:5]  # Check first 5 samples by default
        
        mismatches = []
        for sample_id in samples_to_check:
            # Get ancestry for this sample
            ancestry = carriers.loc[carriers['IID'] == sample_id, 'ancestry'].iloc[0]
            
            # Read corresponding traw file
            traw_path = f'{traw_dir}/{ancestry}_snps.traw'
            traw = self.data_repo.read_csv(traw_path, sep='\t')
            traw_merged = snp_df.merge(traw, how='left', left_on='id', right_on='SNP')
            
            # Check each SNP
            for snp in snp_df['id']:
                # Construct the column name by adding '0_' prefix to the sample_id
                column_name = f'0_{sample_id}'
                
                # Add this debug check after column_name is defined
                if column_name not in traw_merged.columns:
                    print(f"WARNING: Column {column_name} not found in traw_merged")
                    print(f"Available columns: {traw_merged.columns}")
                    # Try finding the column without the '0_' prefix
                    if sample_id in traw_merged.columns:
                        column_name = sample_id
                        print(f"Using {column_name} instead")
                    else:
                        # Try finding columns that contain the sample_id as a substring
                        possible_cols = [col for col in traw_merged.columns if sample_id in col]
                        if possible_cols:
                            column_name = possible_cols[0]
                            print(f"Using closest match: {column_name}")
                        else:
                            print(f"No matching column found for {sample_id}")
                            continue  # Skip this sample-SNP combination
                
                # Get the raw value
                raw_value = traw_merged.loc[
                    traw_merged['id'] == snp, 
                    column_name
                ].iloc[0]
        
                combined_value = carriers.loc[
                    carriers['IID'] == sample_id,
                    snp
                ].iloc[0]
                # print(raw_value, combined_value, snp, sample_id)
                if not self.verify_genotype(raw_value, combined_value, snp):
                    mismatches.append({
                        'sample': sample_id,
                        'ancestry': ancestry,
                        'snp': snp,
                        'raw': raw_value,
                        'combined': combined_value
                    })
        
        # Report results
        if mismatches:
            print("Found mismatches:")
            print(pd.DataFrame(mismatches))
        else:
            print("All checked genotypes match!")
            
        return mismatches