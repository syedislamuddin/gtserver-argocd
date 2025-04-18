import pandas as pd
from typing import Dict, Tuple
from src.core.plink_command import PlinkCommand
from src.core.data_repository import DataRepository
from src.core.genotype_converter import GenotypeConverter
from src.core.carrier_validator import CarrierValidator


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
            var_stats = freq.merge(vmiss[['SNP', 'F_MISS']], on='SNP', how='left')
        except (FileNotFoundError, pd.errors.EmptyDataError):
            print(f"Warning: Missing data file {plink_out}.vmiss not found or empty")
        
        return var_stats


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
        var_stats = self.variant_processor.extract_variants(geno_path, temp_snps_path, plink_out)
        
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
        var_info_df = var_info_df.merge(var_stats, how='left', on='SNP')
        var_info_df.pos = var_info_df.pos.astype(int)
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
            var_info_base.drop(columns=['ALT_x', '#CHROM', 'REF', 'ALT_y'], inplace=True)
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

        # Deduplicate variants based on lowest F_MISS
        var_info_dedup, carriers_string_dedup, carriers_int_dedup = self._deduplicate_variants(
            var_info_base, carriers_string_final, carriers_int_final
        )

        # Define output paths
        carriers_string_path = f"{out_path}_string.csv"
        carriers_int_path = f"{out_path}_int.csv"
        var_info_path = f"{out_path}_info.csv"

        # Save combined files
        self.data_repo.write_csv(carriers_string_dedup, carriers_string_path, index=False)
        self.data_repo.write_csv(carriers_int_dedup, carriers_int_path, index=False)
        self.data_repo.write_csv(var_info_dedup, var_info_path, index=False)

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
            # var_info_base['F_MISS_MAX'] = var_info_base[miss_cols].max(axis=1)

    def _deduplicate_variants(self, var_info: pd.DataFrame,
                              carriers_str: pd.DataFrame,
                              carriers_int: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Select unique variants based on chrom, pos, a1, a2, choosing the one with the lowest F_MISS.

        Args:
            var_info: DataFrame with variant information including 'id', 'chrom', 'pos', 'a1', 'a2', 'F_MISS'.
            carriers_str: DataFrame with carrier strings (columns are 'IID', 'study', 'ancestry', variant 'id's).
            carriers_int: DataFrame with carrier integers (columns are 'IID', 'study', 'ancestry', variant 'id's).

        Returns:
            Tuple containing the deduplicated var_info, carriers_str, and carriers_int DataFrames.
        """
        if 'F_MISS' not in var_info.columns:
            print("Warning: F_MISS column not found in var_info. Skipping deduplication.")
            return var_info, carriers_str, carriers_int

        duplicate_key_cols = ['chrom', 'pos', 'a1', 'a2']

        # Ensure key columns exist
        if not all(col in var_info.columns for col in duplicate_key_cols):
             print(f"Warning: Missing one or more key columns {duplicate_key_cols} in var_info. Skipping deduplication.")
             return var_info, carriers_str, carriers_int

        # Sort by positional key and then by F_MISS to prioritize lowest missingness
        # Handle potential NaN in F_MISS by filling with infinity for sorting (worst missingness)
        var_info_sorted = var_info.sort_values(
            by=duplicate_key_cols + ['F_MISS'],
            ascending=[True] * len(duplicate_key_cols) + [True], # True for F_MISS = lowest first
            na_position='last' # Place NaNs last in F_MISS sorting
        )

        # Identify rows to keep (the first occurrence after sorting by F_MISS)
        ids_to_keep = var_info_sorted.drop_duplicates(subset=duplicate_key_cols, keep='first')['id'].tolist()

        # Filter var_info
        var_info_dedup = var_info[var_info['id'].isin(ids_to_keep)].reset_index(drop=True)

        # Filter carrier data columns
        metadata_cols = ['IID', 'study', 'ancestry']
        variant_cols_to_keep = [col for col in carriers_str.columns if col in ids_to_keep or col in metadata_cols]

        carriers_str_dedup = carriers_str[variant_cols_to_keep]
        carriers_int_dedup = carriers_int[variant_cols_to_keep] # Assumes int df has same columns

        if len(var_info.index) != len(var_info_dedup.index):
            print(f"Deduplicated {len(var_info.index) - len(var_info_dedup.index)} variants based on lowest F_MISS.")

        return var_info_dedup, carriers_str_dedup, carriers_int_dedup
