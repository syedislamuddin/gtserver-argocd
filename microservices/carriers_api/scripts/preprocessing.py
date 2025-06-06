import pandas as pd
import glob

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

# get snps list - original list from UCL
report = pd.read_csv(report_path)
report.columns = [x.strip() for x in report.columns]
report.drop(columns=['GP2ID','Cohort Code', 'Local ID',	'PI','PD EUR PRS (GWAS vXX)','NBA derived Ancestry'], inplace=True)

snp_df1 = report.loc[~report['Unnamed: 0'].isna()]
snp_df1.set_index('Unnamed: 0', inplace=True)
snp_df1 = snp_df1.transpose()
snp_df1.reset_index(inplace=True)
snp_df1.rename(columns={'index':'snp'}, inplace=True)
snp_df1.drop(columns=['Alternative variant name'], inplace=True)
snp_df1.columns.name = None
snp_df1.columns = ['snp','rsid','hg19','hg38','ancestry']
snp_df1 = snp_df1.loc[~snp_df1.hg38.isna()]
# Split the 'snp' column in snp_df1 into 'locus' and 'snp_name' columns
snp_df1[['locus', 'snp_name']] = snp_df1['snp'].str.extract(r'([^\s]+)\s*\(([^)]+)\)')
snp_df1.drop(columns=['snp'],inplace=True)

# read in variant list from Lara
snp_df2 = pd.read_csv('variant_list.csv')

snp_df2.rename(columns={'Unnamed: 11':'pipeline','rsID':'rsid'}, inplace=True)
snp_df2.drop(columns=['snp_name_full','Variant','ID'], inplace=True)
snp_df2.columns = [x.strip() for x in snp_df2.columns]

# remove variants that already exist in snp_df2
snp_df1 = snp_df1.loc[~snp_df1.hg38.isin(snp_df2.hg38)]

# concatenate snp_df1 and snp_df2
snp_df = pd.concat([snp_df1, snp_df2], ignore_index=True)
# replace 'GBA' with 'GBA1'
snp_df['locus'] = snp_df['locus'].apply(lambda x: 'GBA1' if x == 'GBA' else x)

dups = snp_df.duplicated(subset=['hg38', 'snp_name'], keep=False)

# Step 2: For each hg38, collect all unique snp_names
def combine_snp_names(group):
    snp_names = group['snp_name'].dropna().unique()
    if len(snp_names) > 1:
        # If there are multiple snp_names, keep the first and add the second as snp_name2
        row = group.iloc[0].copy()
        row['snp_name_alt'] = snp_names[1]
        return row
    else:
        # If only one snp_name, just return the first row
        row = group.iloc[0].copy()
        row['snp_name_alt'] = np.nan
        return row

# Step 3: Group by hg38 and apply the function
result = snp_df.groupby('hg38', as_index=False, group_keys=False).apply(combine_snp_names, include_groups=False)

# Step 4: Reset index if needed
result = result.reset_index(drop=True)
result[['chrom', 'pos', 'a1', 'a2']] = result['hg38'].str.split(':', expand=True)

snp_df_out = result[
    [

        'snp_name','snp_name_alt','locus','rsid','hg38','hg19', 'chrom','pos','a1','a2',
        'ancestry','submitter_email','precision_medicine','pipeline'
    ]
]
snp_df_out['hg19'] = snp_df_out['hg19'].str.replace(r'\s+', '', regex=True)

duplicate_snp_names = snp_df_out[~snp_df_out.snp_name.isna()].groupby('snp_name').filter(lambda x: x['hg38'].nunique() > 1)
duplicate_snp_names = duplicate_snp_names.sort_values(['snp_name', 'hg38'])
print(f"Found {len(duplicate_snp_names)} variants with same snp_name but different hg38 coordinates")
# print(duplicate_snp_names)
duplicate_snp_names.to_csv(f'{carriers_dir}/duplicate_snp_names.csv', index=False)

snp_df_out = snp_df_out.loc[~snp_df_out.snp_name.isin(duplicate_snp_names.snp_name)]


snp_df_out.to_csv(f'{summary_dir}/carriers_report_snps_full.csv', index=False)





######### TESTING #########
# from services.carriers_api.app.carriers import extract_carriers, combine_carrier_files

# output_dir = f'{data_dir}/api_test/outputs'

# results_by_label = {}
# for label in labels:
#     results = extract_carriers(
#         geno_path=f'{geno_dir}/{label}/{label}_release9_vwb',
#         snplist_path=f'{data_dir}/snps_out.csv',
#         out_path=f'{output_dir}/{label}',
#         return_dfs=True
#     )
#     results_by_label[label] = results

# combined_results = combine_carrier_files(
#     results_by_label=results_by_label,
#     key_file=f'{data_dir}/nba_app_key.csv',
#     output_dir=output_dir
# )