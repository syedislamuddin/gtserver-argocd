from src.core.manager import CarrierAnalysisManager

if __name__ == "__main__":
    # Example configuration

    data_dir = f'/home/vitaled2/gp2_carriers/data'
    report_path = f'{data_dir}/NBA_Report.csv'
    geno_dir = f'{data_dir}/raw_genotypes'
    key_file = f'{data_dir}/nba_app_key.csv'
    output_dir = f'{data_dir}/outputs'
    snplist_path = f'{data_dir}/carriers_report_snps_nba.csv'
    labels = ['AAC', 'AFR', 'AJ', 'AMR', 'CAH', 'CAS', 'EAS', 'EUR', 'FIN', 'MDE', 'SAS']
    
    manager = CarrierAnalysisManager()
    
    # Extract carriers for each ancestry label
    results_by_label = {}
    for label in labels:
        results = manager.extract_carriers(
            geno_path=f'{geno_dir}/{label}/{label}_release9_vwb',
            snplist_path=snplist_path,
            out_path=f'{output_dir}/{label}'
        )
        results_by_label[label] = results
    
    # Combine results
    combined_results = manager.combine_carrier_files(
        results_by_label=results_by_label,
        key_file=key_file,
        out_path=f'{output_dir}/carriers_TEST'
    )
    
    # Validate results
    manager.validate_carrier_data(
        traw_dir=geno_dir,
        combined_file=combined_results['carriers_string'],
        snp_info_file=snplist_path
    )
