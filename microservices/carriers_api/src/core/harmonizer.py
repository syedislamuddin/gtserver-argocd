import pandas as pd
import os
import tempfile
from typing import Optional, List
from src.core.plink_operations import (
    ExtractSnpsCommand, 
    FrequencyCommand, 
    SwapAllelesCommand, 
    UpdateAllelesCommand, 
    ExportCommand, 
    CopyFilesCommand
)


class AlleleHarmonizer:
    def harmonize_and_extract(self, 
                             geno_path: str, 
                             reference_path: Optional[str], 
                             plink_out: str,
                             additional_args: List[str] = None) -> str:
        """
        Harmonize alleles if reference provided, then extract SNPs and execute PLINK operations.
        
        Args:
            geno_path: Path to PLINK file prefix
            reference_path: Path to reference allele file
            plink_out: Output path prefix
            additional_args: Optional list of additional PLINK arguments
            
        Returns:
            str: Path to the subset SNP list file with matched IDs
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            current_geno = geno_path
            
            # Step 1: Find common SNPs between genotype and reference
            common_snps_path = self._find_common_snps(geno_path, reference_path, os.path.join(tmpdir, "common_snps"))
            
            # Step 2: Extract only the common SNPs (for efficiency with large files)
            # Also standardizes chromosome format during extraction
            extracted_prefix = os.path.join(tmpdir, "extracted")
            extract_cmd = ExtractSnpsCommand(geno_path, common_snps_path, extracted_prefix, output_chr='M')
            extract_cmd.execute()
            
            # Step 3: Harmonize alleles on the smaller extracted dataset
            harmonized_prefix = os.path.join(tmpdir, "harmonized")
            match_info_path = os.path.join(tmpdir, "common_snps_match_info.tsv")
            self._harmonize_alleles(extracted_prefix, reference_path, harmonized_prefix, match_info_path)
            current_geno = harmonized_prefix
            
            # Step 4: Build and execute PLINK command with all operations
            export_cmd = ExportCommand(
                pfile=current_geno, 
                out=plink_out, 
                additional_args=additional_args
            )
            export_cmd.execute()
            
            # Create subset SNP list with matched IDs
            subset_snp_path = f"{plink_out}_subset_snps.csv"
            
            # Read the match info to get the mapping between genotype IDs and reference variants
            match_info_path = os.path.join(tmpdir, "common_snps_match_info.tsv")
            match_info = pd.read_csv(match_info_path, sep='\t')
            
            # Read original reference SNP list
            ref_df = pd.read_csv(reference_path, dtype={'chrom': str})
            
            # Use hg38 as variant_id (with uppercase alleles)
            ref_df['hg38'] = ref_df['hg38'].astype(str).str.strip().str.replace(' ', '')
            ref_df['variant_id'] = ref_df['hg38'].str.upper()
            
            # Merge match info with reference data to get subset of matched variants
            subset_snps = match_info.merge(ref_df, left_on='variant_id_ref', right_on='variant_id', how='inner')
            
            # Keep original reference columns plus the genotype ID
            ref_cols = list(ref_df.columns)
            if 'id' not in ref_cols:
                ref_cols.append('id')
            
            # Rename id_geno to id for consistency
            subset_snps = subset_snps.rename(columns={'id_geno': 'id'})
            
            # Select relevant columns, keeping all from original reference
            cols_to_keep = [col for col in ref_cols if col in subset_snps.columns]
            subset_snps = subset_snps[cols_to_keep]
            
            # Save the subset SNP list
            subset_snps.to_csv(subset_snp_path, index=False)
            
            return subset_snp_path
    
    def _find_common_snps(self, pfile: str, reference: str, out: str) -> str:
        """
        Find SNPs common between the PLINK file and reference.
        
        Args:
            pfile: Path to PLINK file prefix
            reference: Path to TSV file with columns chrom, pos, a1, a2
            out: Output path prefix
            
        Returns:
            str: Path to file containing common SNP IDs
        """
        # Read files
        pvar_path = f"{pfile}.pvar"
        
        # Read pvar file - PLINK2 format has header starting with #CHROM
        try:
            # Try reading with header first (PLINK2 format)
            pvar = pd.read_csv(pvar_path, sep='\t', dtype={'#CHROM': str})
            # Rename columns to standard names
            pvar.columns = ['chrom', 'pos', 'id', 'a1', 'a2'] + list(pvar.columns[5:])
        except:
            # Fall back to headerless format
            pvar = pd.read_csv(pvar_path, sep='\t', comment='#', header=None, 
                              names=['chrom', 'pos', 'id', 'a1', 'a2'],
                              usecols=[0, 1, 2, 3, 4],
                              dtype={'chrom': str})
        
        # Clean pvar data
        pvar['chrom'] = pvar['chrom'].astype(str).str.strip()
        pvar['pos'] = pvar['pos'].astype(str).str.strip().str.replace(' ', '')
        pvar['id'] = pvar['id'].astype(str).str.strip()
        pvar['a1'] = pvar['a1'].astype(str).str.strip().str.upper()
        pvar['a2'] = pvar['a2'].astype(str).str.strip().str.upper()
        
        # Create unique variant identifier with alleles
        pvar['variant_id'] = pvar['chrom'] + ':' + pvar['pos'] + ':' + pvar['a1'] + ':' + pvar['a2']
        # Also create position-only identifier for initial filtering
        pvar['pos_id'] = pvar['chrom'] + ':' + pvar['pos']
        
        # Read and clean reference data
        ref_df = pd.read_csv(reference, dtype={'chrom': str})
        
        # Clean hg38 column - handle potential spaces
        ref_df['hg38'] = ref_df['hg38'].astype(str).str.strip().str.replace(' ', '')
        
        # Extract components from hg38 format (e.g., "12:40309225:A:C")
        hg38_parts = ref_df['hg38'].str.split(':')
        ref_df['chrom'] = hg38_parts.str[0]
        ref_df['pos'] = hg38_parts.str[1]
        ref_df['a1'] = hg38_parts.str[2].str.upper()
        ref_df['a2'] = hg38_parts.str[3].str.upper()
        
        # Create identifiers using hg38
        ref_df['pos_id'] = ref_df['chrom'] + ':' + ref_df['pos']
        ref_df['variant_id'] = ref_df['hg38'].str.upper()  # Ensure alleles are uppercase in variant_id
        
        # First, find variants at the same position
        potential_matches = pvar.merge(ref_df, on='pos_id', suffixes=('_geno', '_ref'))
        
        # Debug: print column names to understand the structure
        print(f"DEBUG: Columns after merge: {list(potential_matches.columns)}")
        print(f"DEBUG: Shape after merge: {potential_matches.shape}")
        
        # Check if id_geno exists, if not, it means ref_df didn't have an 'id' column
        if 'id_geno' not in potential_matches.columns and 'id' in potential_matches.columns:
            # The 'id' column from pvar didn't get renamed because ref_df doesn't have 'id'
            potential_matches = potential_matches.rename(columns={'id': 'id_geno'})
        
        # Check all 4 possible orientations
        complement = {'A':'T', 'T':'A', 'C':'G', 'G':'C'}
        
        # Exact match: A1_geno = A1_ref, A2_geno = A2_ref
        exact_match = (
            (potential_matches['a1_geno'] == potential_matches['a1_ref']) & 
            (potential_matches['a2_geno'] == potential_matches['a2_ref'])
        )
        
        # Swap match: A1_geno = A2_ref, A2_geno = A1_ref
        swap_match = (
            (potential_matches['a1_geno'] == potential_matches['a2_ref']) & 
            (potential_matches['a2_geno'] == potential_matches['a1_ref'])
        )
        
        # Flip match: A1_geno complement = A1_ref, A2_geno complement = A2_ref
        flip_match = (
            (potential_matches['a1_geno'].map(lambda x: complement.get(x, x)) == potential_matches['a1_ref']) & 
            (potential_matches['a2_geno'].map(lambda x: complement.get(x, x)) == potential_matches['a2_ref'])
        )
        
        # Flip-swap match: A1_geno complement = A2_ref, A2_geno complement = A1_ref
        flip_swap_match = (
            (potential_matches['a1_geno'].map(lambda x: complement.get(x, x)) == potential_matches['a2_ref']) & 
            (potential_matches['a2_geno'].map(lambda x: complement.get(x, x)) == potential_matches['a1_ref'])
        )
        
        # Keep only true matches
        any_match = exact_match | swap_match | flip_match | flip_swap_match
        true_matches = potential_matches[any_match].copy()
        
        # Debug: check columns in true_matches
        print(f"DEBUG: Columns in true_matches: {list(true_matches.columns)}")
        
        # Ensure id_geno exists in true_matches
        if 'id_geno' not in true_matches.columns:
            print(f"ERROR: id_geno column not found. Available columns: {list(true_matches.columns)}")
            # If we have 'id' but not 'id_geno', rename it
            if 'id' in true_matches.columns:
                true_matches = true_matches.rename(columns={'id': 'id_geno'})
            else:
                raise ValueError("Cannot find variant ID column in merged data")
        
        # Add match type for later harmonization
        true_matches.loc[exact_match[any_match], 'match_type'] = 'exact'
        true_matches.loc[swap_match[any_match], 'match_type'] = 'swap'
        true_matches.loc[flip_match[any_match], 'match_type'] = 'flip'
        true_matches.loc[flip_swap_match[any_match], 'match_type'] = 'flip_swap'
        
        # Remove any duplicates (keep first occurrence)
        true_matches = true_matches.drop_duplicates(subset=['id_geno'])
        
        # Write common SNP IDs to file
        common_snps_path = f"{out}.txt"
        true_matches['id_geno'].to_csv(common_snps_path, index=False, header=False)
        
        # Save match information for harmonization step
        match_info_cols = ['id_geno', 'variant_id_ref', 'match_type']
        # Add reference columns that exist
        for col in ['a1_ref', 'a2_ref', 'snp_name_ref']:
            if col in true_matches.columns:
                match_info_cols.append(col)
        
        match_info_path = f"{out}_match_info.tsv"
        true_matches[match_info_cols].to_csv(match_info_path, sep='\t', index=False)
        
        return common_snps_path
    
    def _extract_snps(self, pfile: str, snps_file: str, out: str) -> None:
        """
        Extract specified SNPs from PLINK file.
        
        Args:
            pfile: Path to PLINK file prefix
            snps_file: Path to file with SNP IDs to extract
            out: Output path prefix
        """
        extract_cmd = ExtractSnpsCommand(pfile, snps_file, out)
        extract_cmd.execute()
    
    def _harmonize_alleles(self, pfile: str, reference: str, out: str, match_info_path: str) -> None:
        """
        Harmonize alleles in PLINK files to match reference alleles and ensure alt alleles are minor alleles.
        
        Args:
            pfile: Path to PLINK file prefix
            reference: Path to TSV file with columns chrom, pos, a1, a2
            out: Output path prefix
            match_info_path: Path to match information file
        """
        # Read match information
        match_info = pd.read_csv(match_info_path, sep='\t')
        
        # Extract SNP IDs by match type
        swap_mask = match_info['match_type'].isin(['swap', 'flip_swap'])
        flip_mask = match_info['match_type'].isin(['flip', 'flip_swap'])
        
        with tempfile.TemporaryDirectory() as nested_tmpdir:
            current_pfile = pfile
            update_alleles_path = os.path.join(nested_tmpdir, "update_alleles.txt")
            swap_path = os.path.join(nested_tmpdir, "swap_snps.txt")
            updated_pfile = os.path.join(nested_tmpdir, "updated")
            
            # Handle flipping
            flip_snps = match_info[flip_mask]
            if not flip_snps.empty:
                # Read pvar to get current alleles
                pvar_path = f"{pfile}.pvar"
                try:
                    # Try reading with header first (PLINK2 format)
                    pvar = pd.read_csv(pvar_path, sep='\t', dtype={'#CHROM': str})
                    # Rename columns to standard names
                    pvar.columns = ['chrom', 'pos', 'id', 'a1', 'a2'] + list(pvar.columns[5:])
                except:
                    # Fall back to headerless format
                    pvar = pd.read_csv(pvar_path, sep='\t', comment='#', header=None,
                                      names=['chrom', 'pos', 'id', 'a1', 'a2'],
                                      usecols=[0, 1, 2, 3, 4],
                                      dtype={'chrom': str})
                
                # Merge with flip SNPs to get alleles
                flip_snps_with_alleles = flip_snps.merge(pvar, left_on='id_geno', right_on='id')
                
                # Prepare update alleles file
                complement = {'A':'T', 'T':'A', 'C':'G', 'G':'C'}
                update_alleles = flip_snps_with_alleles[['id', 'a1', 'a2']].copy()
                update_alleles['new_a1'] = update_alleles['a1'].map(lambda x: complement.get(x, x))
                update_alleles['new_a2'] = update_alleles['a2'].map(lambda x: complement.get(x, x))
                
                # Write the update alleles file with 5 columns: ID, old-A1, old-A2, new-A1, new-A2
                update_alleles[['id', 'a1', 'a2', 'new_a1', 'new_a2']].to_csv(
                    update_alleles_path, sep='\t', index=False, header=False)
                
                # Execute update alleles command
                update_cmd = UpdateAllelesCommand(pfile, update_alleles_path, updated_pfile)
                update_cmd.execute()
                current_pfile = updated_pfile
            
            # Handle swapping
            swap_snps = match_info[swap_mask]
            if not swap_snps.empty:
                # For swap operations, we need to know which allele to make REF
                # We'll use the reference a1 as the target REF allele
                swap_snps[['id_geno', 'a1_ref']].to_csv(swap_path, sep='\t', index=False, header=False)
                reference_adjusted = os.path.join(nested_tmpdir, "reference_adjusted")
                
                swap_cmd = SwapAllelesCommand(current_pfile, swap_path, reference_adjusted)
                swap_cmd.execute()
                current_pfile = reference_adjusted
            
            # Run frequency calculation
            freq_prefix = os.path.join(nested_tmpdir, "freq_check")
            freq_cmd = FrequencyCommand(current_pfile, freq_prefix)
            freq_cmd.execute()
            
            # Process frequency data
            freq_df = pd.read_csv(f"{freq_prefix}.afreq", sep='\t')
            variants_to_swap = freq_df[freq_df['ALT_FREQS'] > 0.5][['ID', 'REF']]
            
            if not variants_to_swap.empty:
                minor_swap_path = os.path.join(nested_tmpdir, "minor_allele_swap.txt")
                variants_to_swap.to_csv(minor_swap_path, sep='\t', index=False, header=False)
                
                # Execute swap command
                minor_swap_cmd = SwapAllelesCommand(current_pfile, minor_swap_path, out)
                minor_swap_cmd.execute()
            else:
                # Just copy files
                copy_cmd = CopyFilesCommand(current_pfile, out)
                copy_cmd.execute() 