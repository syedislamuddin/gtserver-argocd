import subprocess
import pandas as pd
import os
import tempfile
from typing import Optional, List


class PlinkOperations:
    def harmonize_and_extract(self, 
                             geno_path: str, 
                             reference_path: Optional[str], 
                             plink_out: str,
                             additional_args: List[str] = None) -> None:
        """
        Harmonize alleles if reference provided, then extract SNPs and execute PLINK operations.
        
        Args:
            geno_path: Path to PLINK file prefix
            reference_path: Path to reference allele file
            plink_out: Output path prefix
            additional_args: Optional list of additional PLINK arguments
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            current_geno = geno_path
            
            # Step 1: Find common SNPs between genotype and reference
            common_snps_path = self._find_common_snps(geno_path, reference_path, os.path.join(tmpdir, "common_snps"))
            
            # Step 2: Extract only the common SNPs (for efficiency with large files)
            extracted_prefix = os.path.join(tmpdir, "extracted")
            self._extract_snps(geno_path, common_snps_path, extracted_prefix)
            
            # Step 3: Harmonize alleles on the smaller extracted dataset
            harmonized_prefix = os.path.join(tmpdir, "harmonized")
            self._harmonize_alleles(extracted_prefix, reference_path, harmonized_prefix)
            current_geno = harmonized_prefix
            
            # Step 4: Build and execute PLINK command with all operations
            cmd_parts = [
                f"plink2 --pfile {current_geno}",
                "--export Av",
                "--freq",
                "--missing"
            ]
            
            # Add any additional arguments
            if additional_args:
                cmd_parts.extend(additional_args)
                
            # Add output path
            cmd_parts.append(f"--out {plink_out}")
            
            # Execute command
            full_cmd = " ".join(cmd_parts)
            subprocess.run(full_cmd, shell=True, check=True)
    
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
        pvar = pd.read_csv(pvar_path, sep='\t', dtype={'#CHROM': str})
        pvar.columns = ['chrom', 'pos', 'id', 'a1', 'a2']
        pvar.loc[:, 'snpid'] = pvar['chrom'] + ':' + pvar['pos'].astype(str)
        
        ref_df = pd.read_csv(reference, sep='\t', dtype={'chrom': str})
        ref_df.loc[:, 'snpid'] = ref_df['chrom'] + ':' + ref_df['pos'].astype(str)
        
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
        cmd = f"plink2 --pfile {pfile} --extract {snps_file} --make-pgen --out {out}"
        subprocess.run(cmd, shell=True, check=True)
    
    def _harmonize_alleles(self, pfile: str, reference: str, out: str) -> None:
        """
        Harmonize alleles in PLINK files to match reference alleles.
        
        Args:
            pfile: Path to PLINK file prefix
            reference: Path to TSV file with columns chrom, pos, a1, a2
            out: Output path prefix
        """
        # Read files
        pvar_path = f"{pfile}.pvar"
        pvar = pd.read_csv(pvar_path, sep='\t', dtype={'#CHROM': str})
        pvar.columns = ['chrom', 'pos', 'id', 'a1_x', 'a2_x']
        pvar.loc[:, 'snpid'] = pvar['chrom'] + ':' + pvar['pos'].astype(str)
        
        ref_df = pd.read_csv(reference, sep='\t', dtype={'chrom': str})
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
        
        # Create temporary files for PLINK operations
        with tempfile.TemporaryDirectory() as nested_tmpdir:
            flip_path = os.path.join(nested_tmpdir, "flip_snps.txt")
            swap_path = os.path.join(nested_tmpdir, "swap_snps.txt")
            flip_temp_pfile = os.path.join(nested_tmpdir, "flipped")
            
            # Write flip list (both regular flip and flip+swap)
            flip_snps = merged.loc[flip_mask | flip_swap_mask, 'id']
            if not flip_snps.empty:
                flip_snps.to_csv(flip_path, index=False, header=False)
                
                # Step 1: Flip alleles
                cmd = f"plink2 --pfile {pfile} --flip {flip_path} --make-pgen --out {flip_temp_pfile}"
                subprocess.run(cmd, shell=True, check=True)
                
                # Use the flipped file for next step
                current_pfile = flip_temp_pfile
            else:
                # No flipping needed
                current_pfile = pfile
            
            # Write swap list (including those that need both flip and swap)
            swap_snps = merged.loc[swap_mask | flip_swap_mask, ['id', 'a1']]
            if not swap_snps.empty:
                swap_snps.to_csv(swap_path, sep='\t', index=False, header=False)
                
                # Step 2: Swap A1/A2 designations
                cmd = f"plink2 --pfile {current_pfile} --a1-allele {swap_path} 2 1 --make-pgen --out {out}"
                subprocess.run(cmd, shell=True, check=True)
            elif current_pfile != pfile:
                # If we flipped but don't need to swap, rename the temporary files
                cmd = f"cp {current_pfile}.pgen {out}.pgen && cp {current_pfile}.pvar {out}.pvar && cp {current_pfile}.psam {out}.psam"
                subprocess.run(cmd, shell=True, check=True)
            else:
                # If no changes needed, copy original files
                cmd = f"cp {pfile}.pgen {out}.pgen && cp {pfile}.pvar {out}.pvar && cp {pfile}.psam {out}.psam"
                subprocess.run(cmd, shell=True, check=True)