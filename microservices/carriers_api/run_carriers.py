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

    print("\n=== All processing complete! ===")

if __name__ == "__main__":
    main()
