import requests
import json
from IPython.display import display, HTML

def format_error_response(response_text):
    try:
        # Parse the JSON response
        response_data = json.loads(response_text)
        
        # Extract the error detail
        if 'detail' in response_data:
            error_detail = response_data['detail']
            
            # Split the error message from the traceback
            if '\n\n\nTraceback:' in error_detail:
                error_message, traceback = error_detail.split('\n\n\nTraceback:', 1)
            else:
                error_message = error_detail
                traceback = ""
            
            # Display the error message
            print("Error Message:")
            print(f"  {error_message.strip()}")
            
            # Format the traceback with syntax highlighting
            if traceback:
                html = f"""
                <div style="margin-top: 10px;">
                    <details>
                        <summary style="cursor: pointer; color: #cc0000; font-weight: bold;">Show Traceback</summary>
                        <pre style="background-color: #f8f8f8; padding: 10px; border-radius: 5px; margin-top: 10px; overflow: auto; max-height: 500px;"><code>{traceback}</code></pre>
                    </details>
                </div>
                """
                display(HTML(html))
        else:
            # Pretty-print the entire JSON
            formatted_json = json.dumps(response_data, indent=2)
            html = f"""
            <pre style="background-color: #f8f8f8; padding: 10px; border-radius: 5px; overflow: auto;">{formatted_json}</pre>
            """
            display(HTML(html))
    except json.JSONDecodeError:
        # If not valid JSON, just print the text
        print(response_text)


def post_to_api(url, payload):
    # Make the API request
    response = requests.post(url, json=payload)
    print(f"Status code: {response.status_code}")
    
    # Handle the response
    if response.status_code == 200:
        data = response.json()
        formatted_json = json.dumps(data, indent=2)
        html = f"""
        <div style="background-color: #f0fff0; padding: 10px; border-radius: 5px; border: 1px solid #ccc;">
            <h3 style="margin-top: 0;">Success!</h3>
            <pre style="margin-bottom: 0;">{formatted_json}</pre>
        </div>
        """
        display(HTML(html))
        
        # Display file paths if available
        if 'outputs' in data:
            print("\nOutput Files:")
            for key, path in data['outputs'].items():
                print(f"- {key}: {path}")
    else:
        # Format error response
        try:
            format_error_response(response.text)
        except:
            print(f"Error: {response.text}")

import pandas as pd

# gcsfuse --implicit-dirs gp2tier2_vwb ~/gcs_mounts/gp2tier2_vwb
# gcsfuse --implicit-dirs genotools-server ~/gcs_mounts/genotools_server

release = '9'
mnt_dir = '/home/vitaled2/gcs_mounts'

# carriers dirs in mount
carriers_dir = f'{mnt_dir}/genotools_server/carriers'
summary_dir = f'{carriers_dir}/summary_data'
nba_out_dir = f'{carriers_dir}/nba'
nba_pgen = f'{nba_out_dir}/GP2_r10_v2_ready_genotools'
imputed_out_dir = f'{carriers_dir}/imputed'
wgs_out_dir = f'{carriers_dir}/wgs'
report_path = f'{summary_dir}/NBA_Report.csv'

# release dirs in mount
release_dir = f'{mnt_dir}/gp2tier2_vwb/release9_18122024'
nba_release_dir = f'{release_dir}/raw_genotypes'
imputed_release_dir = f'{release_dir}/imputed_genotypes'
clinical_dir = f'{release_dir}/clinical_data'


key_file = f'{clinical_dir}/master_key_release9_final_vwb.csv'
labels = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN','MDE', 'SAS']

key = pd.read_csv(key_file)

import requests
for label in labels:
# Test health endpoint
    response = requests.get("http://localhost:8000/health")
    print(f"Health check: {response.json()}")

    # Example carrier request (adjust parameters as needed)
    payload = {
        "geno_path": f"{nba_release_dir}/{label}/{label}_release9_vwb",
        "key_file_path": key_file,
        "snplist_path": f'{summary_dir}/carriers_report_snps_full.csv',
        "out_path": f"{nba_out_dir}/{label}_release9",
        "release_version": "9"
    }

    api_url = "http://localhost:8000/process_carriers"
    post_to_api(api_url, payload)            