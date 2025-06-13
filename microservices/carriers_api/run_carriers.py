import pandas as pd
import os
import requests
import glob
import argparse
from src.core.api_utils import format_error_response, post_to_api

# gcsfuse --implicit-dirs gp2tier2_vwb ~/gcs_mounts/gp2tier2_vwb
# gcsfuse --implicit-dirs genotools-server ~/gcs_mounts/genotools_server
# python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

def cleanup_directory(directory_path, cleanup_enabled):
    """Remove existing files in the directory if cleanup is enabled."""
    print("*"*100)
    if not cleanup_enabled:
        print(f"Cleanup disabled, skipping removal of existing files in {directory_path}")
        return
    
    existing_files = glob.glob(f'{directory_path}/*')
    for file_path in existing_files:
        try:
            os.remove(file_path)
            print(f"Removed existing file: {file_path}")
        except OSError as e:
            print(f"Error removing file {file_path}: {e}")
    print("*"*100)

def combine_array_data_outputs(nba_carriers_release_out_dir, labels, release, output_dir):
    """
    Combine all array data outputs from individual populations into 3 consolidated files.
    
    Args:
        nba_carriers_release_out_dir: Base directory containing population-specific outputs
        labels: List of population labels (e.g., ['AAC', 'AFR', ...])
        release: Release version string
        output_dir: Directory where combined files will be saved
    
    Returns:
        dict: Paths to the 3 combined output files
    """
    print(f"\n=== Combining Array Data Outputs ===")
    
    # Initialize combined dataframes
    combined_carriers_string = pd.DataFrame()
    combined_carriers_int = pd.DataFrame() 
    combined_var_info = pd.DataFrame()
    
    # Track which var_info columns we've seen to handle population-specific frequency columns
    var_info_base = None
    
    for i, label in enumerate(labels):
        print(f"Processing {label} for combination...")
        
        # Define file paths for this population
        var_info_path = f"{nba_carriers_release_out_dir}/{label}/{label}_release{release}_var_info.csv"
        carriers_string_path = f"{nba_carriers_release_out_dir}/{label}/{label}_release{release}_carriers_string.csv"
        carriers_int_path = f"{nba_carriers_release_out_dir}/{label}/{label}_release{release}_carriers_int.csv"
        
        # Check if files exist
        if not all(os.path.exists(path) for path in [var_info_path, carriers_string_path, carriers_int_path]):
            print(f"Warning: Missing files for {label}, skipping...")
            continue
            
        # Read the files
        var_info = pd.read_csv(var_info_path)
        carriers_string = pd.read_csv(carriers_string_path)
        carriers_int = pd.read_csv(carriers_int_path)
        
        # Process var_info (combine frequency information from all populations)
        if var_info_base is None:
            # First population - use as base and drop frequency columns
            var_info_base = var_info.copy()
            freq_cols = ['ALT_FREQS', 'OBS_CT']
            if 'F_MISS' in var_info_base.columns:
                freq_cols.append('F_MISS')
            var_info_base = var_info_base.drop(columns=freq_cols, errors='ignore')
            combined_var_info = var_info_base.copy()
        
        # Add population-specific frequency columns
        combined_var_info[f'ALT_FREQS_{label}'] = var_info['ALT_FREQS']
        combined_var_info[f'OBS_CT_{label}'] = var_info['OBS_CT']
        if 'F_MISS' in var_info.columns:
            combined_var_info[f'F_MISS_{label}'] = var_info['F_MISS']
        
        # Track which variant IDs were used in carriers data for this ancestry
        # For duplicates with same snp_name, the one with lowest F_MISS is used
        combined_var_info[f'{label}_probe_used'] = False  # Initialize all as False
        
        if 'snp_name' in var_info.columns and f'F_MISS_{label}' in combined_var_info.columns:
            # Group by snp_name and select the variant with lowest F_MISS for each
            for snp_name in combined_var_info['snp_name'].unique():
                if pd.isna(snp_name):
                    continue
                    
                snp_group = combined_var_info[combined_var_info['snp_name'] == snp_name]
                if len(snp_group) > 0:
                    # Find the variant with lowest F_MISS for this ancestry
                    miss_col = f'F_MISS_{label}'
                    if miss_col in snp_group.columns:
                        # Sort by F_MISS (ascending = lowest first), handle NaN
                        best_variant_idx = snp_group[miss_col].idxmin()
                        if pd.notna(best_variant_idx):
                            combined_var_info.loc[best_variant_idx, f'{label}_probe_used'] = True
                        else:
                            # If all F_MISS are NaN, just pick the first one
                            combined_var_info.loc[snp_group.index[0], f'{label}_probe_used'] = True
        else:
            # Fallback: if no F_MISS data, mark all variants as used
            combined_var_info[f'{label}_probe_used'] = True
        
        # Add detailed logging
        used_ids = combined_var_info[combined_var_info[f'{label}_probe_used']]['id'].tolist()
        unused_ids = combined_var_info[~combined_var_info[f'{label}_probe_used']]['id'].tolist()
        
        print(f"{label}: {len(used_ids)} variants used in carriers data, {len(unused_ids)} variants not used")
        
        # Check for duplicate snp_names that had different IDs selected
        if 'snp_name' in combined_var_info.columns:
            dup_snp_names = combined_var_info[combined_var_info['snp_name'].duplicated(keep=False)]
            if not dup_snp_names.empty:
                for snp_name in dup_snp_names['snp_name'].unique():
                    if pd.isna(snp_name):
                        continue
                        
                    snp_variants = combined_var_info[combined_var_info['snp_name'] == snp_name]
                    used_variants = snp_variants[snp_variants[f'{label}_probe_used']]
                    
                    if len(snp_variants) > 1 and len(used_variants) > 0:
                        used_id = used_variants.iloc[0]['id']
                        used_f_miss = used_variants.iloc[0][f'F_MISS_{label}'] if f'F_MISS_{label}' in used_variants.columns else 'N/A'
                        unused_variants = snp_variants[~snp_variants[f'{label}_probe_used']]
                        unused_info = []
                        for _, row in unused_variants.iterrows():
                            f_miss_val = row[f'F_MISS_{label}'] if f'F_MISS_{label}' in row else 'N/A'
                            unused_info.append(f"{row['id']} (F_MISS={f_miss_val})")
                        print(f"  {label} - {snp_name}: Used {used_id} (F_MISS={used_f_miss}), skipped {unused_info}")

        # Process carriers data (add ancestry column and combine)
        carriers_string['ancestry'] = label
        carriers_int['ancestry'] = label
        
        # Clean IID column (remove '0_' prefix if present)
        carriers_string['IID'] = carriers_string['IID'].str.replace('0_', '')
        carriers_int['IID'] = carriers_int['IID'].str.replace('0_', '')
        
        # Combine with previous data
        combined_carriers_string = pd.concat([combined_carriers_string, carriers_string], ignore_index=True)
        combined_carriers_int = pd.concat([combined_carriers_int, carriers_int], ignore_index=True)
    
    # Calculate aggregate statistics for var_info
    print("Calculating aggregate statistics...")
    
    # Get frequency and observation columns
    freq_cols = [col for col in combined_var_info.columns if col.startswith('ALT_FREQS_')]
    obs_cols = [col for col in combined_var_info.columns if col.startswith('OBS_CT_')]
    miss_cols = [col for col in combined_var_info.columns if col.startswith('F_MISS_')]
    
    # Calculate total observations
    combined_var_info['OBS_CT'] = combined_var_info[obs_cols].sum(axis=1)
    
    # Calculate weighted average allele frequency
    weighted_sum = pd.Series(0, index=combined_var_info.index)
    for i, freq_col in enumerate(freq_cols):
        matching_obs_col = obs_cols[i]
        weighted_sum += combined_var_info[freq_col] * combined_var_info[matching_obs_col]
    
    # Avoid division by zero
    combined_var_info['ALT_FREQS'] = weighted_sum / combined_var_info['OBS_CT'].replace(0, float('nan'))
    
    # Calculate weighted average missingness (NEW - properly weighted)
    if miss_cols:
        miss_weighted_sum = pd.Series(0, index=combined_var_info.index)
        for i, miss_col in enumerate(miss_cols):
            matching_obs_col = obs_cols[i]
            miss_weighted_sum += combined_var_info[miss_col] * combined_var_info[matching_obs_col]
        
        # Weighted average missingness
        combined_var_info['F_MISS'] = miss_weighted_sum / combined_var_info['OBS_CT'].replace(0, float('nan'))
        
        # Optional: Also keep simple average for comparison
        combined_var_info['F_MISS_SIMPLE_AVG'] = combined_var_info[miss_cols].mean(axis=1)
    
    # Handle NaN values in frequency columns after all calculations
    print("Handling NaN values in frequency columns...")
    
    # Set NaN to 0.0 for ALT_FREQS columns when corresponding OBS_CT > 0
    for i, freq_col in enumerate(freq_cols):
        matching_obs_col = obs_cols[i]
        
        # Find rows where frequency is NaN but observations > 0
        nan_mask = combined_var_info[freq_col].isna()
        valid_obs_mask = combined_var_info[matching_obs_col] > 0
        fix_mask = nan_mask & valid_obs_mask
        
        if fix_mask.sum() > 0:
            print(f"Setting {fix_mask.sum()} NaN values to 0.0 in {freq_col} where {matching_obs_col} > 0")
            combined_var_info.loc[fix_mask, freq_col] = 0.0
    
    # Set NaN to 0.0 for F_MISS columns when corresponding OBS_CT > 0
    for i, miss_col in enumerate(miss_cols):
        matching_obs_col = obs_cols[i]
        
        # Find rows where missingness is NaN but observations > 0
        nan_mask = combined_var_info[miss_col].isna()
        valid_obs_mask = combined_var_info[matching_obs_col] > 0
        fix_mask = nan_mask & valid_obs_mask
        
        if fix_mask.sum() > 0:
            print(f"Setting {fix_mask.sum()} NaN values to 0.0 in {miss_col} where {matching_obs_col} > 0")
            combined_var_info.loc[fix_mask, miss_col] = 0.0
    
    # Also handle the aggregate ALT_FREQS and F_MISS columns
    if 'ALT_FREQS' in combined_var_info.columns:
        nan_mask = combined_var_info['ALT_FREQS'].isna()
        valid_obs_mask = combined_var_info['OBS_CT'] > 0
        fix_mask = nan_mask & valid_obs_mask
        
        if fix_mask.sum() > 0:
            print(f"Setting {fix_mask.sum()} NaN values to 0.0 in aggregate ALT_FREQS where OBS_CT > 0")
            combined_var_info.loc[fix_mask, 'ALT_FREQS'] = 0.0
    
    if 'F_MISS' in combined_var_info.columns:
        nan_mask = combined_var_info['F_MISS'].isna()
        valid_obs_mask = combined_var_info['OBS_CT'] > 0
        fix_mask = nan_mask & valid_obs_mask
        
        if fix_mask.sum() > 0:
            print(f"Setting {fix_mask.sum()} NaN values to 0.0 in aggregate F_MISS where OBS_CT > 0")
            combined_var_info.loc[fix_mask, 'F_MISS'] = 0.0

    # Reorder carriers dataframes columns 
    variant_columns = [col for col in combined_carriers_string.columns if col not in ['IID', 'ancestry']]
    combined_carriers_string = combined_carriers_string[['IID', 'ancestry'] + variant_columns]
    combined_carriers_int = combined_carriers_int[['IID', 'ancestry'] + variant_columns]
    
    # Fill missing values in carriers data
    combined_carriers_string[variant_columns] = combined_carriers_string[variant_columns].fillna('./.')
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Define output file paths
    var_info_output = f"{output_dir}/nba_release{release}_combined_var_info.csv"
    carriers_string_output = f"{output_dir}/nba_release{release}_combined_carriers_string.csv"
    carriers_int_output = f"{output_dir}/nba_release{release}_combined_carriers_int.csv"
    
    # Save combined files
    print(f"Saving combined files to {output_dir}...")
    combined_var_info.to_csv(var_info_output, index=False)
    combined_carriers_string.to_csv(carriers_string_output, index=False)
    combined_carriers_int.to_csv(carriers_int_output, index=False)
    
    print(f"✓ Combined var_info: {var_info_output}")
    print(f"✓ Combined carriers_string: {carriers_string_output}")
    print(f"✓ Combined carriers_int: {carriers_int_output}")
    
    return {
        'var_info': var_info_output,
        'carriers_string': carriers_string_output,
        'carriers_int': carriers_int_output
    }

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process carrier data for multiple populations')
    parser.add_argument('--cleanup', type=bool, default=True,
                        help='Remove existing files in output directories before processing (default: True)')
    args = parser.parse_args()

    mnt_dir = '/home/vitaled2/gcs_mounts'
    carriers_dir = f'{mnt_dir}/genotools_server/carriers'
    summary_dir = f'{carriers_dir}/summary_data'

    release = '10'

    # Check API health once
    response = requests.get("http://localhost:8000/health")
    print(f"Health check: {response.json()}")
    print(f"Cleanup enabled: {args.cleanup}")

    # Process array data (NBA)
    print("\n=== Processing Array Data (NBA) ===")
    release_dir = '/home/vitaled2/gcs_mounts/gp2_release10_staging/vwb/raw_genotypes/'
    nba_carriers_release_out_dir = f'/home/vitaled2/gcs_mounts/genotools_server/carriers/nba/release{release}'

    labels = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN','MDE', 'SAS']

    for label in labels:
        print(f"\nProcessing {label}...")
        os.makedirs(f'{nba_carriers_release_out_dir}/{label}', exist_ok=True)

        # Remove existing files in the output directory if cleanup is enabled
        cleanup_directory(f'{nba_carriers_release_out_dir}/{label}', args.cleanup)
                
        payload = {
            "geno_path": f"{release_dir}/{label}/{label}_release{release}_vwb",
            "snplist_path": f'{summary_dir}/carriers_report_snps_full.csv',
            "out_path": f"{nba_carriers_release_out_dir}/{label}/{label}_release{release}",
            "release_version": "10"
        }

        api_url = "http://localhost:8000/process_carriers"
        post_to_api(api_url, payload)

    # Process WGS data
    print("\n=== Processing WGS Data ===")
    wgs_dir = f'{mnt_dir}/genotools_server/carriers/wgs/raw_genotypes'
    wgs_carriers_release_out_dir = f'{mnt_dir}/genotools_server/carriers/wgs/release{release}'

    os.makedirs(f'{wgs_carriers_release_out_dir}', exist_ok=True)
    # Remove existing files in the output directory if cleanup is enabled
    cleanup_directory(wgs_carriers_release_out_dir, args.cleanup)

    payload = {
        "geno_path": f"{wgs_dir}/R10_wgs_carrier_vars",
        "snplist_path": f'{summary_dir}/carriers_report_snps_full.csv',
        "out_path": f"{wgs_carriers_release_out_dir}/release{release}",
        "release_version": "10"
    }

    api_url = "http://localhost:8000/process_carriers"
    post_to_api(api_url, payload)
    
    # Combine array data outputs into consolidated files
    print("\n=== Combining Array Data Outputs ===")
    combined_output_dir = f'{mnt_dir}/genotools_server/carriers/nba/release{release}/combined'
    combined_results = combine_array_data_outputs(
        nba_carriers_release_out_dir, 
        labels, 
        release, 
        combined_output_dir
    )

    print("\n=== All processing complete! ===")
    print(f"Individual population files saved in: {nba_carriers_release_out_dir}")
    print(f"Combined array data files saved in: {combined_output_dir}")

if __name__ == "__main__":
    main()
