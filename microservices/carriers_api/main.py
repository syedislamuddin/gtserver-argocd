from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from google.cloud import storage
import tempfile
import os
# from typing import List
from src.core.manager import CarrierAnalysisManager
from src.core.security import get_api_key
import uvicorn

app = FastAPI()

class CarrierRequest(BaseModel):
    raw_geno_path: str  # Base GCS path (e.g., "gs://gp2_carriers/api_test/raw_genotypes")
    snplist_path: str  # GCS path to SNP list file
    key_file_path: str  # GCS path to key file
    output_path: str  # Full GCS path including desired prefix (e.g., "gs://bucket/path/prefix_name")

def download_from_gcs(bucket_name: str, blob_path: str, local_path: str):
    """Download a file from GCS to local storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(local_path)

def upload_to_gcs(bucket_name: str, local_path: str, blob_path: str):
    """Upload a file from local storage to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

ANCESTRY_LABELS = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN', 'MDE', 'SAS']

@app.post("/process_carriers")
async def process_carriers(
    request: CarrierRequest,
    api_key: str = Depends(get_api_key)
):
    """
    Process carrier information from genotype files stored in GCS.
    Returns paths to the generated files in GCS.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up directory structure
            geno_dir = os.path.join(temp_dir, "genotypes")
            output_dir = os.path.join(temp_dir, "outputs")
            os.makedirs(geno_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            # Download SNP list and key file
            snplist_local = os.path.join(temp_dir, "snps.csv")
            key_file_local = os.path.join(temp_dir, "key.csv")
            
            bucket, blob_path = request.snplist_path.replace("gs://", "").split("/", 1)
            download_from_gcs(bucket, blob_path, snplist_local)
            
            bucket, blob_path = request.key_file_path.replace("gs://", "").split("/", 1)
            download_from_gcs(bucket, blob_path, key_file_local)

            # Process each ancestry label
            results_by_label = {}
            base_bucket = request.raw_geno_path.replace("gs://", "").split("/")[0]
            base_prefix = "/".join(request.raw_geno_path.replace("gs://", "").split("/")[1:])
            
            # Store individual label results
            label_results = {}
            manager = CarrierAnalysisManager()
            
            for label in ANCESTRY_LABELS:
                # Create ancestry-specific directories
                label_dir = os.path.join(geno_dir, label)
                os.makedirs(label_dir, exist_ok=True)

                # Download genotype files (.pgen, .pvar, .psam)
                for ext in ['pgen', 'pvar', 'psam']:
                    gcs_path = f"{base_prefix}/{label}/{label}_release9_vwb.{ext}"
                    local_path = os.path.join(label_dir, f"{label}_release9_vwb.{ext}")
                    download_from_gcs(base_bucket, gcs_path, local_path)

                # Process carriers for this ancestry
                results = manager.extract_carriers(
                    geno_path=os.path.join(label_dir, f"{label}_release9_vwb"),
                    snplist_path=snplist_local,
                    out_path=os.path.join(output_dir, label)
                )
                results_by_label[label] = results
                
                # Upload individual label results to GCS
                label_gcs_results = {}
                output_bucket = request.output_path.replace("gs://", "").split("/")[0]
                output_prefix = "/".join(request.output_path.replace("gs://", "").split("/")[1:])
                
                for key, local_path in results.items():
                    if key.endswith('_df'):  # Skip DataFrame objects
                        continue
                    filename = os.path.basename(local_path)
                    gcs_path = f"{output_prefix}/{label}/{filename}"
                    upload_to_gcs(output_bucket, local_path, gcs_path)
                    label_gcs_results[key] = f"gs://{output_bucket}/{gcs_path}"
                
                label_results[label] = label_gcs_results

            # Create a local output path for combined results
            output_base_name = os.path.basename(request.output_path)
            combined_local_prefix = os.path.join(output_dir, output_base_name)
            
            # Combine results with the local output path
            combined_results = manager.combine_carrier_files(
                results_by_label=results_by_label,
                key_file=key_file_local,
                out_path=combined_local_prefix
            )

            # Upload combined results to GCS
            gcs_results = {}
            output_bucket = request.output_path.replace("gs://", "").split("/")[0]
            output_prefix = "/".join(request.output_path.replace("gs://", "").split("/")[1:])
            
            for key, local_path in combined_results.items():
                filename = os.path.basename(local_path)
                gcs_path = f"{output_prefix}/{filename}"
                upload_to_gcs(output_bucket, local_path, gcs_path)
                gcs_results[key] = f"gs://{output_bucket}/{gcs_path}"

            return {
                "status": "success",
                "processed_labels": ANCESTRY_LABELS,
                "label_outputs": label_results,
                "combined_outputs": gcs_results
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test_local_processing")
async def test_local_processing():
    """Endpoint to trigger the local carrier processing logic for testing."""
    try:
        # Hardcoded paths for local testing
        data_dir = f'/home/vitaled2/gp2_carriers/data'
        # report_path = f'{data_dir}/NBA_Report.csv' # Not used in the selected code
        geno_dir = f'{data_dir}/raw_genotypes'
        key_file = f'{data_dir}/nba_app_key.csv'
        output_dir = f'{data_dir}/outputs'
        snplist_path = f'{data_dir}/carriers_report_snps_nba.csv'
        labels = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN', 'MDE', 'SAS']

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        manager = CarrierAnalysisManager()

        # Extract carriers for each ancestry label
        results_by_label = {}
        for label in labels:
            label_output_dir = os.path.join(output_dir, label)
            os.makedirs(label_output_dir, exist_ok=True)
            results = manager.extract_carriers(
                geno_path=f'{geno_dir}/{label}/{label}_release9_vwb',
                snplist_path=snplist_path,
                out_path=label_output_dir
            )
            results_by_label[label] = results

        # Combine results
        combined_out_path_prefix = os.path.join(output_dir, 'carriers_TEST_local')
        combined_results = manager.combine_carrier_files(
            results_by_label=results_by_label,
            key_file=key_file,
            out_path=combined_out_path_prefix # Use a prefix for combined files
        )

        return {
            "status": "success",
            "message": "Local processing test completed.",
            "combined_output_files": combined_results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Local processing failed: {str(e)}")

if __name__ == "__main__":
    # # data_dir = f'/home/vitaled2/gp2_carriers/data'
    # # report_path = f'{data_dir}/NBA_Report.csv'
    # # geno_dir = f'{data_dir}/raw_genotypes'
    # # key_file = f'{data_dir}/nba_app_key.csv'
    # # output_dir = f'{data_dir}/outputs'
    # # snplist_path = f'{data_dir}/carriers_report_snps_nba.csv'
    # # labels = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN', 'MDE', 'SAS']
    # 
    # # manager = CarrierAnalysisManager()
    # 
    # # # Extract carriers for each ancestry label
    # # results_by_label = {}
    # # for label in labels:
    # #     results = manager.extract_carriers(
    # #         geno_path=f'{geno_dir}/{label}/{label}_release9_vwb',
    # #         snplist_path=snplist_path,
    # #         out_path=f'{output_dir}/{label}'
    # #     )
    # #     results_by_label[label] = results
    # 
    # # # Combine results
    # # combined_results = manager.combine_carrier_files(
    # #     results_by_label=results_by_label,
    # #     key_file=key_file,
    # #     out_path=f'{output_dir}/carriers_TEST'
    # # )
    # 
    # 
    uvicorn.run(app, host="0.0.0.0", port=8000)
