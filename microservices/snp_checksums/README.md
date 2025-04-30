## Running the CLI (`main.py`)

### Required Input
- `--bfile` / `--pfile`: Specify the input bfile or pfile.
  - If a bfile is provided, the process will convert it to a pfile.

### SNP Selection Options
- If neither `--snp_list` nor `--default_snps` is used, the process will call `plink --geno 0.0` to generate a list of SNPs with perfect callrate.
- `--snp_list`: Provide a custom list of SNPs.
- `--default_snps`: Use a pre-determined SNP list.
  - **Note:** Not recommended unless you are certain all your samples have all the SNPs in this list.

### Output
- `--out`: Specify the output file path.

---

## General Workflow

1. **SNP Selection:**  
   Obtain a list of SNPs with perfect (100%) callrate across all samples.
   - *Example:* In GP2 R6, ~200 SNPs were called in every sample, which was sufficient to identify all duplicates in the entire release.

2. **Generate Raw File:**  
   Extract the selected SNPs and generate a raw genotype file.

3. **Concatenate Genotypes:**  
   Concatenate the raw genotype calls for each sample into a string.

4. **Hash Genotype Strings:**  
   Hash the genotype string for each sample.

5. **Duplicate Detection:**  
   Compare the hashed values among samples to identify duplicates.

6. **Output:**  
   - Hashed values are saved in a `.txt` file.
   - Found duplicates are listed in a `.json` file.

---

## Considerations

- **Missing Genotypes:**  
  - If a sample has a "NC" (no call) at any SNP, its concatenated genotype string will produce a different hash value, potentially causing missed duplicates.
  - A `missing_alleles.txt` file is generated listing all samples with missing alleles, and a warning message is printed.

- **Floating Point Issues:**  
  - In GP2 R6, there was a case where one SNP was not called in a single sample even after running `--geno 0.0`.
  - **Solution:** Rerunning `--geno 0.0` resolved the issue.
