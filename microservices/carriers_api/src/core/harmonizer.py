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
            self._harmonize_alleles(extracted_prefix, reference_path, harmonized_prefix)
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
            
            # Read the final harmonized pvar file to get final SNP IDs
            pvar_path = f"{current_geno}.pvar"
            # Read pvar file with proper handling of header and extra columns
            pvar = pd.read_csv(pvar_path, sep='\t', comment='#', header=None, 
                              names=['chrom', 'pos', 'id', 'a1', 'a2'],
                              usecols=[0, 1, 2, 3, 4],
                              dtype={'chrom': str})
            
            # Read original reference SNP list
            ref_df = pd.read_csv(reference_path, dtype={'chrom': str})
            
            # Make sure both have snpid columns for merging
            pvar.loc[:, 'snpid'] = pvar['chrom'] + ':' + pvar['pos'].astype(str)
            
            if 'snpid' not in ref_df.columns:
                ref_df.loc[:, 'snpid'] = ref_df['chrom'] + ':' + ref_df['pos'].astype(str)
            
            # Merge to get the subset of reference SNPs that were actually used
            subset_snps = ref_df.merge(pvar[['id', 'snpid']], on='snpid', how='inner')
            
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
        # Read pvar file with proper handling of header and extra columns
        pvar = pd.read_csv(pvar_path, sep='\t', comment='#', header=None, 
                          names=['chrom', 'pos', 'id', 'a1', 'a2'],
                          usecols=[0, 1, 2, 3, 4],
                          dtype={'chrom': str})
        
        # Clean pvar data
        pvar['chrom'] = pvar['chrom'].astype(str).str.strip()
        pvar['pos'] = pvar['pos'].astype(str).str.strip().str.replace(' ', '')
        pvar['id'] = pvar['id'].astype(str).str.strip()
        pvar.loc[:, 'snpid'] = pvar['chrom'] + ':' + pvar['pos']
        
        # Read and clean reference data
        ref_df = pd.read_csv(reference, dtype={'chrom': str})
        
        # Clean ref_df data - handle potential spaces in coordinates
        for col in ref_df.columns:
            if col in ['chrom', 'pos', 'id', 'a1', 'a2', 'hg19', 'hg38']:
                ref_df[col] = ref_df[col].astype(str).str.strip().str.replace(' ', '')
        
        # Create snpid for reference
        if 'chrom' in ref_df.columns and 'pos' in ref_df.columns:
            ref_df.loc[:, 'snpid'] = ref_df['chrom'] + ':' + ref_df['pos']
        elif 'hg19' in ref_df.columns:
            # Handle hg19 format like "1:1234567:A:T"
            ref_df.loc[:, 'snpid'] = ref_df['hg19'].str.split(':', n=2).str[:2].str.join(':')
        elif 'hg38' in ref_df.columns:
            # Handle hg38 format
            ref_df.loc[:, 'snpid'] = ref_df['hg38'].str.split(':', n=2).str[:2].str.join(':')
        
        # Merge data to find common variants
        merged = pvar.merge(ref_df, how='inner', on='snpid')
        
        # Write common SNP IDs to file
        common_snps_path = f"{out}.txt"
        merged['id'].to_csv(common_snps_path, index=False, header=False)
        
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
    
    def _harmonize_alleles(self, pfile: str, reference: str, out: str) -> None:
        """
        Harmonize alleles in PLINK files to match reference alleles and ensure alt alleles are minor alleles.
        
        Args:
            pfile: Path to PLINK file prefix
            reference: Path to TSV file with columns chrom, pos, a1, a2
            out: Output path prefix
        """
        # Read files
        pvar_path = f"{pfile}.pvar"
        # Read pvar file with proper handling of header and extra columns
        pvar = pd.read_csv(pvar_path, sep='\t', comment='#', header=None,
                          names=['chrom', 'pos', 'id', 'a1_x', 'a2_x'],
                          usecols=[0, 1, 2, 3, 4],
                          dtype={'chrom': str})
        pvar.loc[:, 'snpid'] = pvar['chrom'] + ':' + pvar['pos'].astype(str)
        
        ref_df = pd.read_csv(reference, dtype={'chrom': str})
        ref_df.loc[:, 'snpid'] = ref_df['chrom'] + ':' + ref_df['pos'].astype(str)
        
        # Merge data
        merged = pvar.merge(ref_df, how='inner', on='snpid', suffixes=('_x', '_y'))
        
        # Create masks for different allele scenarios
        swap_mask = (merged['a1_x'] == merged['a2']) & (merged['a2_x'] == merged['a1'])
        
        complement = {'A':'T', 'T':'A', 'C':'G', 'G':'C'}
        flip_mask = (merged['a1_x'].map(lambda x: complement.get(x, x)) == merged['a1']) & \
                    (merged['a2_x'].map(lambda x: complement.get(x, x)) == merged['a2'])
        
        flip_swap_mask = (merged['a1_x'].map(lambda x: complement.get(x, x)) == merged['a2']) & \
                         (merged['a2_x'].map(lambda x: complement.get(x, x)) == merged['a1'])
        
        with tempfile.TemporaryDirectory() as nested_tmpdir:
            current_pfile = pfile
            update_alleles_path = os.path.join(nested_tmpdir, "update_alleles.txt")
            swap_path = os.path.join(nested_tmpdir, "swap_snps.txt")
            updated_pfile = os.path.join(nested_tmpdir, "updated")
            
            # Handle flipping
            flip_snps = merged.loc[flip_mask | flip_swap_mask]
            if not flip_snps.empty:
                # Prepare update alleles file
                update_alleles = flip_snps[['id', 'a1_x', 'a2_x']].copy()
                # Calculate complement alleles for both A1 and A2
                update_alleles['new_a1'] = update_alleles['a1_x'].map(lambda x: complement.get(x, x))
                update_alleles['new_a2'] = update_alleles['a2_x'].map(lambda x: complement.get(x, x))
                
                # Write the update alleles file with 5 columns: ID, old-A1, old-A2, new-A1, new-A2
                update_alleles[['id', 'a1_x', 'a2_x', 'new_a1', 'new_a2']].to_csv(
                    update_alleles_path, sep='\t', index=False, header=False)
                
                # Execute update alleles command
                update_cmd = UpdateAllelesCommand(pfile, update_alleles_path, updated_pfile)
                update_cmd.execute()
                current_pfile = updated_pfile
            
            # Handle swapping
            swap_snps = merged.loc[swap_mask | flip_swap_mask, ['id', 'a1']]
            if not swap_snps.empty:
                swap_snps.to_csv(swap_path, sep='\t', index=False, header=False)
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