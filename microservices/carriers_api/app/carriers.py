import pandas as pd
import subprocess
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any


# Strategy Pattern for Genotype Conversion
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


# Command Pattern for External Tool Execution
class PlinkCommand:
    def execute(self, geno_path: str, temp_snps_path: str, plink_out: str) -> None:
        """Execute a PLINK command"""
        extract_cmd = f"plink2 --pfile {geno_path} --extract {temp_snps_path} --export Av --freq --missing --out {plink_out}"
        subprocess.run(extract_cmd, shell=True, check=True)


# Repository Pattern for Data Access
class DataRepository:
    @staticmethod
    def read_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """Read data from CSV file"""
        return pd.read_csv(file_path, **kwargs)
    
    @staticmethod
    def write_csv(data: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Write data to CSV file"""
        data.to_csv(file_path, **kwargs)
    
    @staticmethod
    def remove_file(file_path: str) -> None:
        """Remove a file"""
        if os.path.exists(file_path):
            os.remove(file_path)


# Factory Pattern for creating processing strategies
class CarrierProcessorFactory:
    @staticmethod
    def create_variant_processor(plink_command: PlinkCommand, data_repo: DataRepository) -> 'VariantProcessor':
        """Create a VariantProcessor instance"""
        return VariantProcessor(plink_command, data_repo)
    
    @staticmethod
    def create_carrier_extractor(variant_processor: 'VariantProcessor', 
                                genotype_converter: GenotypeConverter,
                                data_repo: DataRepository) -> 'CarrierExtractor':
        """Create a CarrierExtractor instance"""
        return CarrierExtractor(variant_processor, genotype_converter, data_repo)
    
    @staticmethod
    def create_carrier_combiner(data_repo: DataRepository) -> 'CarrierCombiner':
        """Create a CarrierCombiner instance"""
        return CarrierCombiner(data_repo)
    
    @staticmethod
    def create_validator(data_repo: DataRepository, 
                         genotype_converter: GenotypeConverter) -> 'CarrierValidator':
        """Create a CarrierValidator instance"""
        return CarrierValidator(data_repo, genotype_converter)


# Processing Components (utilize Strategy pattern internally)
class VariantProcessor:
    def __init__(self, plink_command: PlinkCommand, data_repo: DataRepository):
        self.plink_command = plink_command
        self.data_repo = data_repo
    
    def extract_variants(self, geno_path: str, temp_snps_path: str, plink_out: str) -> pd.DataFrame:
        """
        Extract variant statistics using plink2 from genotype data.
        
        Args:
            geno_path: Path to the plink2 pfile prefix (without extensions)
            temp_snps_path: Path to file containing SNPs to extract
            plink_out: Prefix for output files
            
        Returns:
            pd.DataFrame: Combined variant statistics with frequency and missingness data
        """
        self.plink_command.execute(geno_path, temp_snps_path, plink_out)
        
        # Read frequency data
        freq = self.data_repo.read_csv(f"{plink_out}.afreq", sep='\t')
        freq.rename(columns={'ID':'SNP'}, inplace=True)
        
        # Read variant missingness data
        try:
            vmiss = self.data_repo.read_csv(f"{plink_out}.vmiss", sep='\t')
            vmiss.rename(columns={'ID':'SNP'}, inplace=True)
            
            # Merge frequency and missingness data
            freq = freq.merge(vmiss[['SNP', 'F_MISS']], on='SNP', how='left')
        except (FileNotFoundError, pd.errors.EmptyDataError):
            print(f"Warning: Missing data file {plink_out}.vmiss not found or empty")
        
        return freq


class CarrierExtractor:
    def __init__(self, variant_processor: VariantProcessor, 
                 genotype_converter: GenotypeConverter,
                 data_repo: DataRepository):
        self.variant_processor = variant_processor
        self.genotype_converter = genotype_converter
        self.data_repo = data_repo

    def extract_carriers(self, geno_path: str, snplist_path: str, out_path: str) -> Dict[str, str]:
        """
        Extract carrier information for given SNPs from a PLINK2 dataset.
        
        Args:
            geno_path: Path to PLINK2 files prefix (without .pgen/.pvar/.psam extension)
            snplist_path: Path to file containing SNP information
            out_path: Output path prefix for generated files
        
        Returns:
            dict: Paths to generated carrier files
        """
        # Read SNP information
        snp_df = self.data_repo.read_csv(snplist_path)
        
        # Prepare temporary files
        temp_snps_path = f"{out_path}_temp_snps.txt"
        snp_df['id'].to_csv(temp_snps_path, header=False, index=False)
        plink_out = f"{out_path}_snps"
        
        # Extract variant statistics
        freq = self.variant_processor.extract_variants(geno_path, temp_snps_path, plink_out)
        
        # Read and process traw data
        traw = self.data_repo.read_csv(f"{plink_out}.traw", sep='\t')
        traw_merged = snp_df.merge(traw, how='left', left_on='id', right_on='SNP')
        
        # Define column sets
        colnames = ['id', 'chrom', 'pos', 'a1', 'a2','CHR', 'SNP', '(C)M', 'POS', 'COUNTED', 'ALT']
        var_cols = [x for x in colnames if x not in ['id']]
        sample_cols = list(traw_merged.drop(columns=colnames).columns)
        
        # Process final traw data
        traw_final = traw_merged.loc[:, colnames + sample_cols]
        
        # Create string format output
        traw_out = traw_final.copy()
        traw_out[sample_cols] = traw_out.apply(
            lambda row: [self.genotype_converter.convert(row[col], row['id']) for col in sample_cols],
            axis=1,
            result_type='expand'
        )
        
        # Process and save frequency info
        var_info_df = traw_final.loc[:, colnames]
        var_info_df = var_info_df.merge(freq, how='left', on='SNP')
        self.data_repo.write_csv(var_info_df, f"{out_path}_var_info.csv", index=False)
        
        # Process and save string format
        carriers_string = traw_out.drop(columns=var_cols).set_index('id').T.reset_index()
        carriers_string.columns.name = None
        carriers_string = carriers_string.fillna('./.')
        carriers_string = carriers_string.astype(str)
        carriers_string.rename(columns={'index':'IID'}, inplace=True)
        self.data_repo.write_csv(carriers_string, f"{out_path}_carriers_string.csv", index=False)
        
        # Process and save integer format
        carriers_int = traw_final.drop(columns=var_cols).set_index('id').T.reset_index()
        carriers_int.columns.name = None
        carriers_int.rename(columns={'index':'IID'}, inplace=True)
        self.data_repo.write_csv(carriers_int, f"{out_path}_carriers_int.csv", index=False)
        
        # Clean up temporary files
        self.data_repo.remove_file(temp_snps_path)
        
        return {
            'var_info': f"{out_path}_var_info.csv",
            'carriers_string': f"{out_path}_carriers_string.csv",
            'carriers_int': f"{out_path}_carriers_int.csv"
        }


class CarrierCombiner:
    def __init__(self, data_repo: DataRepository):
        self.data_repo = data_repo

    def combine_carrier_files(self, results_by_label: Dict[str, Dict[str, str]], 
                             key_file: str, out_path: str) -> Dict[str, str]:
        """
        Combine carrier files from multiple ancestry labels into consolidated output files.
        
        Args:
            results_by_label: Dictionary mapping ancestry labels to their extract_carriers results
            key_file: Path to key file containing study information
            out_path: Path prefix for output files
        
        Returns:
            dict: Paths to generated output files
        """
        carriers_string_full = pd.DataFrame()
        carriers_int_full = pd.DataFrame()
        
        # Get base variant info from first label and drop frequency columns
        first_label_results = next(iter(results_by_label.values()))
        var_info_key = 'var_info_df' if 'var_info_df' in first_label_results else 'var_info'
        var_info_base = first_label_results[var_info_key]
        
        if isinstance(var_info_base, str):
            var_info_base = self.data_repo.read_csv(var_info_base)
            
        freq_cols = ['ALT_FREQS', 'OBS_CT']
        var_info_base = var_info_base.drop(columns=freq_cols, errors='ignore')
        var_info_base.drop_duplicates(inplace=True)
        
        # Read key file
        key = self.data_repo.read_csv(key_file)
        
        # Process each ancestry label's results
        for label, results in results_by_label.items():
            # Read data files
            label_var_info = self.data_repo.read_csv(results['var_info'])
            carriers_string = self.data_repo.read_csv(results['carriers_string'])
            carriers_int = self.data_repo.read_csv(results['carriers_int'])
            
            # Add frequency data for this population
            var_info_base[f'ALT_FREQS_{label}'] = label_var_info['ALT_FREQS']
            var_info_base[f'OBS_CT_{label}'] = label_var_info['OBS_CT']
            
            # Add missingness data if available
            if 'F_MISS' in label_var_info.columns:
                var_info_base[f'F_MISS_{label}'] = label_var_info['F_MISS']
            
            # Process string format carriers
            carriers_string['IID'] = carriers_string['IID'].str.replace('0_', '')
            carriers_string.loc[:,'ancestry'] = label
            carriers_string_full = pd.concat([carriers_string_full, carriers_string], ignore_index=True)
            
            # Process integer format carriers
            carriers_int['IID'] = carriers_int['IID'].str.replace('0_', '')
            carriers_int.loc[:,'ancestry'] = label
            carriers_int_full = pd.concat([carriers_int_full, carriers_int], ignore_index=True)
        
        # Process and combine data
        variant_columns = [x for x in carriers_string_full.columns if x not in ['IID','ancestry']]
        
        # Process string format output
        carriers_string_full_out = carriers_string_full[['IID', 'ancestry'] + variant_columns]
        carriers_string_full_out[variant_columns] = carriers_string_full_out[variant_columns].fillna('./.')
        carriers_string_full_out_merge = carriers_string_full_out.merge(key[['IID','study']], how='left', on='IID')
        carriers_string_final = carriers_string_full_out_merge[['IID', 'study', 'ancestry'] + variant_columns]
        
        # Process integer format output
        carriers_int_full_out = carriers_int_full[['IID', 'ancestry'] + variant_columns]
        carriers_int_full_out_merge = carriers_int_full_out.merge(key[['IID','study']], how='left', on='IID')
        carriers_int_final = carriers_int_full_out_merge[['IID', 'study', 'ancestry'] + variant_columns]
        
        # Calculate aggregate statistics
        self._calculate_aggregate_statistics(var_info_base)
        
        # Define output paths
        carriers_string_path = f"{out_path}_string.csv"
        carriers_int_path = f"{out_path}_int.csv"
        var_info_path = f"{out_path}_info.csv"
        
        # Save combined files
        self.data_repo.write_csv(carriers_string_final, carriers_string_path, index=False)
        self.data_repo.write_csv(carriers_int_final, carriers_int_path, index=False)
        self.data_repo.write_csv(var_info_base, var_info_path, index=False)
        
        return {
            'carriers_string': carriers_string_path,
            'carriers_int': carriers_int_path,
            'var_info': var_info_path
        }
    
    def _calculate_aggregate_statistics(self, var_info_base: pd.DataFrame) -> None:
        """Calculate aggregate statistics across all ancestries"""
        # Handle frequency calculations
        freq_cols = [col for col in var_info_base.columns if col.startswith('ALT_FREQS_')]
        obs_cols = [col for col in var_info_base.columns if col.startswith('OBS_CT_')]
        
        # Sum up total observations
        var_info_base['OBS_CT'] = var_info_base[obs_cols].sum(axis=1)
        
        # Calculate weighted average for allele frequencies
        weighted_sum = pd.Series(0, index=var_info_base.index)
        for i, freq_col in enumerate(freq_cols):
            matching_obs_col = obs_cols[i]
            weighted_sum += var_info_base[freq_col] * var_info_base[matching_obs_col]
        
        # Avoid division by zero
        var_info_base['ALT_FREQS'] = weighted_sum / var_info_base['OBS_CT'].replace(0, float('nan'))
        
        # Handle missingness calculations
        miss_cols = [col for col in var_info_base.columns if col.startswith('F_MISS_')]
        if miss_cols:
            # Calculate average missingness rate across all ancestry groups
            var_info_base['F_MISS'] = var_info_base[miss_cols].mean(axis=1)
            
            # Find maximum missingness rate for each variant
            var_info_base['F_MISS_MAX'] = var_info_base[miss_cols].max(axis=1)


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
            traw_path = f'{traw_dir}/{ancestry}/{ancestry}_release9_vwb_snps.traw'
            traw = self.data_repo.read_csv(traw_path, sep='\t')
            traw_merged = snp_df.merge(traw, how='left', left_on='id', right_on='SNP')
            
            # Check each SNP
            for snp in snp_df['id']:
                raw_value = traw_merged.loc[
                    traw_merged['id'] == snp, 
                    f'0_{sample_id}'
                ].iloc[0]
                
                combined_value = carriers.loc[
                    carriers['IID'] == sample_id,
                    snp
                ].iloc[0]
                
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


# Facade Pattern to provide a simple interface to the client
class CarrierAnalysisFacade:
    def __init__(self):
        # Initialize common dependencies
        self.data_repo = DataRepository()
        self.plink_command = PlinkCommand()
        self.genotype_converter = StandardGenotypeConverter()
        
        # Initialize processors using the factory
        factory = CarrierProcessorFactory()
        self.variant_processor = factory.create_variant_processor(self.plink_command, self.data_repo)
        self.carrier_extractor = factory.create_carrier_extractor(
            self.variant_processor, self.genotype_converter, self.data_repo)
        self.carrier_combiner = factory.create_carrier_combiner(self.data_repo)
        self.validator = factory.create_validator(self.data_repo, self.genotype_converter)
    
    def extract_carriers(self, geno_path: str, snplist_path: str, out_path: str) -> Dict[str, str]:
        """Extract carrier information for given SNPs"""
        return self.carrier_extractor.extract_carriers(geno_path, snplist_path, out_path)
    
    def combine_carrier_files(self, results_by_label: Dict[str, Dict[str, str]], 
                             key_file: str, out_path: str) -> Dict[str, str]:
        """Combine carrier files from multiple ancestry labels"""
        return self.carrier_combiner.combine_carrier_files(results_by_label, key_file, out_path)
    
    def validate_carrier_data(self, traw_dir: str, combined_file: str, 
                             snp_info_file: str, samples_to_check: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Validate that combined carrier data matches original traw files"""
        return self.validator.validate_carrier_data(traw_dir, combined_file, snp_info_file, samples_to_check)


# Example usage:
if __name__ == "__main__":
    # Example configuration

    data_dir = f'/home/vitaled2/gp2_carriers/data'
    report_path = f'{data_dir}/NBA_Report.csv'
    geno_dir = f'{data_dir}/raw_genotypes'
    key_file = f'{data_dir}/nba_app_key.csv'
    output_dir = f'{data_dir}/api_test/outputs'
    snplist_path = f'{data_dir}/carriers_report_snps_nba.csv'
    labels = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN', 'MDE', 'SAS']
    
    # Create facade
    facade = CarrierAnalysisFacade()
    
    # Extract carriers for each ancestry label
    results_by_label = {}
    for label in labels:
        results = facade.extract_carriers(
            geno_path=f'{geno_dir}/{label}/{label}_release9_vwb',
            snplist_path=snplist_path,
            out_path=f'{output_dir}/{label}'
        )
        results_by_label[label] = results
    
    # Combine results
    combined_results = facade.combine_carrier_files(
        results_by_label=results_by_label,
        key_file=key_file,
        out_path=f'{output_dir}/combined'
    )
    
    # Validate results
    facade.validate_carrier_data(
        traw_dir=geno_dir,
        combined_file=combined_results['carriers_string'],
        snp_info_file=snplist_path
    )