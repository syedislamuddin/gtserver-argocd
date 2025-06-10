import pandas as pd
from typing import Dict, Tuple
from src.core.harmonizer import AlleleHarmonizer
from src.core.data_repository import DataRepository
from src.core.genotype_converter import GenotypeConverter
from src.core.carrier_validator import CarrierValidator


class CarrierProcessorFactory:
    @staticmethod
    def create_variant_processor(harmonizer: AlleleHarmonizer, data_repo: DataRepository) -> 'VariantProcessor':
        """Create a VariantProcessor instance"""
        return VariantProcessor(harmonizer, data_repo)
    
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
    def __init__(self, harmonizer: AlleleHarmonizer, data_repo: DataRepository):
        self.harmonizer = harmonizer
        self.data_repo = data_repo
    
    def extract_variants(self, geno_path: str, reference_path: str, plink_out: str) -> Tuple[pd.DataFrame, str]:
        """
        Extract variant statistics using plink2 from genotype data.
        
        Args:
            geno_path: Path to the plink2 pfile prefix (without extensions)
            reference_path: Path to reference allele file for harmonization
            plink_out: Prefix for output files
            
        Returns:
            Tuple[pd.DataFrame, str]: Combined variant statistics with frequency/missingness data and path to subset SNP file
        """
        # Use the updated harmonize_and_extract method that returns subset SNP path
        subset_snp_path = self.harmonizer.harmonize_and_extract(geno_path, reference_path, plink_out)
        
        # Initialize empty DataFrame for results
        var_stats = pd.DataFrame()
        
        try:
            # Read frequency data
            freq = self.data_repo.read_csv(f"{plink_out}.afreq", sep='\t')
            freq.rename(columns={'ID':'SNP'}, inplace=True)
            var_stats = freq
            
            # Read variant missingness data
            try:
                vmiss = self.data_repo.read_csv(f"{plink_out}.vmiss", sep='\t')
                vmiss.rename(columns={'ID':'SNP'}, inplace=True)
                
                # Merge frequency and missingness data
                var_stats = freq.merge(vmiss[['SNP', 'F_MISS']], on='SNP', how='left')
            except (FileNotFoundError, pd.errors.EmptyDataError):
                print(f"Warning: Missing data file {plink_out}.vmiss not found or empty")
                # var_stats remains freq-only
                
        except (FileNotFoundError, pd.errors.EmptyDataError):
            print(f"Warning: Frequency data file {plink_out}.afreq not found or empty")
            # Check if at least we have missingness data
            try:
                vmiss = self.data_repo.read_csv(f"{plink_out}.vmiss", sep='\t')
                vmiss.rename(columns={'ID':'SNP'}, inplace=True)
                var_stats = vmiss
                print(f"Found missingness data but no frequency data")
            except (FileNotFoundError, pd.errors.EmptyDataError):
                print(f"Error: Neither frequency nor missingness data available")
                # Return empty DataFrame with expected column
                var_stats = pd.DataFrame(columns=['SNP'])
        
        return var_stats, subset_snp_path


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
        # Read SNP information to get the id to snp_name mapping
        snp_list_df = self.data_repo.read_csv(snplist_path)
        
        # Prepare prefix for PLINK output
        plink_out = f"{out_path}_snps"
        
        # Extract variant statistics using the SNP list as reference and get subset SNP path
        var_stats, subset_snp_path = self.variant_processor.extract_variants(geno_path, snplist_path, plink_out)
        
        # Read the subset SNP list that contains matched IDs
        subset_snp_df = self.data_repo.read_csv(subset_snp_path)
        
        # Read and process traw data
        traw = self.data_repo.read_csv(f"{plink_out}.traw", sep='\t')
        
        # Use the subset SNP dataframe for merging instead of the original
        traw_merged = subset_snp_df[['id','chrom','pos','a1','a2']].merge(traw, how='left', left_on='id', right_on='SNP')
        
        # Add snp_name to traw_merged if available in subset_snp_df
        if 'snp_name' in subset_snp_df.columns:
            traw_merged = traw_merged.merge(subset_snp_df[['id', 'snp_name']], on='id', how='left')
            # Define column sets including snp_name
            colnames = ['id', 'snp_name', 'chrom', 'pos', 'a1', 'a2','CHR', 'SNP', '(C)M', 'POS', 'COUNTED', 'ALT']
        else:
            # Define column sets without snp_name
            colnames = ['id', 'chrom', 'pos', 'a1', 'a2','CHR', 'SNP', '(C)M', 'POS', 'COUNTED', 'ALT']
            
        var_cols = [x for x in colnames if x not in ['id']]
        sample_cols = list(traw_merged.drop(columns=colnames).columns)
        
        # Process final traw data
        traw_final = traw_merged.loc[:, colnames + sample_cols]
        
        # Create string format output - use snp_name for genotype conversion if available
        traw_out = traw_final.copy()
        if 'snp_name' in traw_final.columns:
            # Use snp_name for genotype naming
            traw_out[sample_cols] = traw_out.apply(
                lambda row: [self.genotype_converter.convert(row[col], row['snp_name']) for col in sample_cols],
                axis=1,
                result_type='expand'
            )
        else:
            # Fall back to using id for genotype naming
            traw_out[sample_cols] = traw_out.apply(
                lambda row: [self.genotype_converter.convert(row[col], row['id']) for col in sample_cols],
                axis=1,
                result_type='expand'
            )
        
        # Create comprehensive variant information file by combining subset_snp metadata with PLINK statistics
        # 
        # HARMONIZATION EXPLANATION:
        # During harmonization, alleles are matched between input reference and genotype data:
        # - Original input alleles (ref_allele_orig, alt_allele_orig) are from your input SNP list
        # - PLINK alleles (plink_counted, plink_alt) are from the harmonized genotype data
        # - These may differ due to strand flips, allele swaps, or reference/alt designation
        # - PLINK uses "COUNTED" allele for frequency calculations (equivalent to REF)
        # - Genotype calls in carriers files are relative to the harmonized PLINK alleles
        comprehensive_var_info = subset_snp_df.copy()
        
        # Add PLINK statistics (frequency and missingness data)
        if not var_stats.empty and 'SNP' in var_stats.columns:
            comprehensive_var_info = comprehensive_var_info.merge(var_stats, left_on='id', right_on='SNP', how='left')
            # Remove redundant SNP column
            comprehensive_var_info.drop(columns=['SNP'], inplace=True, errors='ignore')
        
        # Add PLINK variant metadata from traw_final with clear naming to distinguish harmonized vs original
        traw_metadata_cols = ['CHR', '(C)M', 'POS', 'COUNTED', 'ALT']
        available_metadata_cols = [col for col in traw_metadata_cols if col in traw_final.columns]
        if available_metadata_cols:
            plink_metadata = traw_final[['id'] + available_metadata_cols].copy()
            # Rename PLINK columns to make it clear these are from the harmonized genotype data
            plink_metadata.rename(columns={
                'CHR': 'plink_chr',           # Chromosome from PLINK (should match chr)
                'POS': 'plink_pos',           # Position from PLINK (should match pos)
                'ALT': 'plink_alt_allele',    # ALT allele after harmonization 
                '(C)M': 'plink_cm',           # Genetic distance in centiMorgans
                'COUNTED': 'plink_ref_allele' # REF allele after harmonization (used for frequency calc)
            }, inplace=True)
            comprehensive_var_info = comprehensive_var_info.merge(plink_metadata, on='id', how='left')
        
        # Clean up and reorganize columns for clarity
        # Remove redundant columns and make naming clearer
        columns_to_drop = ['variant_id', 'plink_chr', 'plink_pos', 'plink_cm', 'plink_ref_allele', 'plink_alt_allele']  # Remove redundant PLINK columns
        comprehensive_var_info.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        
        # Rename columns to match final desired output
        column_renames = {
            'a1': 'a1_ref',                   # Original reference allele from input SNP list
            'a2': 'a2_ref',                   # Original alt allele from input SNP list  
            'chrom': 'chrom',                 # Keep chromosome as 'chrom'
            '#CHROM': 'chrom'                 # Rename #CHROM to chrom if it exists (will be overwritten)
        }
        comprehensive_var_info.rename(columns=column_renames, inplace=True)
        
        # Remove any remaining redundant chromosome columns - keep only 'chrom'
        redundant_chr_cols = ['#CHROM', 'CHR', 'chrom.1']
        comprehensive_var_info.drop(columns=redundant_chr_cols, inplace=True, errors='ignore')
        
        # Ensure pos is integer if it exists
        if 'pos' in comprehensive_var_info.columns:
            comprehensive_var_info['pos'] = comprehensive_var_info['pos'].astype(int)
        
        # Save the comprehensive variant information file (replaces both var_info and subset_snps)
        var_info_output_path = f"{out_path}_var_info.csv"
        self.data_repo.write_csv(comprehensive_var_info, var_info_output_path, index=False)

        # Process and save string format
        carriers_string = traw_out.drop(columns=var_cols).set_index('id').T.reset_index()
        carriers_string.columns.name = None
        carriers_string = carriers_string.fillna('./.')
        carriers_string = carriers_string.astype(str)
        carriers_string.rename(columns={'index':'IID'}, inplace=True)
        carriers_string.loc[:,'IID'] = carriers_string.loc[:,'IID'].str.replace('0_', '')
        
        # Rename variant columns from id to snp_name if available
        if 'snp_name' in subset_snp_df.columns:
            id_to_snp_name = subset_snp_df.set_index('id')['snp_name'].to_dict()
            carriers_string = carriers_string.rename(columns=id_to_snp_name)
        
        self.data_repo.write_csv(carriers_string, f"{out_path}_carriers_string.csv", index=False)
        
        # Process and save integer format
        carriers_int = traw_final.drop(columns=var_cols).set_index('id').T.reset_index()
        carriers_int.columns.name = None
        carriers_int.rename(columns={'index':'IID'}, inplace=True)
        carriers_int.loc[:,'IID'] = carriers_int.loc[:,'IID'].str.replace('0_', '')
        
        # Rename variant columns from id to snp_name if available
        if 'snp_name' in subset_snp_df.columns:
            carriers_int = carriers_int.rename(columns=id_to_snp_name)
        
        self.data_repo.write_csv(carriers_int, f"{out_path}_carriers_int.csv", index=False)
        
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

        # Deduplicate variants based on lowest F_MISS
        var_info_dedup, carriers_string_dedup, carriers_int_dedup = self._deduplicate_variants(
            var_info_base, carriers_string_final, carriers_int_final
        )
        
        # Clean up duplicate columns - remove _y versions and rename _x versions
        columns_to_drop = []
        columns_to_rename = {}
        
        for col in var_info_dedup.columns:
            if col.endswith('_y'):
                columns_to_drop.append(col)
            elif col.endswith('_x'):
                # Rename _x version to original name
                original_name = col[:-2]  # Remove '_x' suffix
                columns_to_rename[col] = original_name
        
        # Drop _y columns and rename _x columns
        var_info_dedup.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        var_info_dedup.rename(columns=columns_to_rename, inplace=True)
        
        # Also drop other redundant columns that might still be present
        redundant_columns = ['#CHROM', 'REF', 'variant_id']
        var_info_dedup.drop(columns=redundant_columns, inplace=True, errors='ignore')

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
