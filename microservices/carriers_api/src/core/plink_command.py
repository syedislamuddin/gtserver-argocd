import subprocess


class PlinkCommand:
    def execute(self, geno_path: str, temp_snps_path: str, plink_out: str) -> None:
        """Execute a PLINK command"""
        extract_cmd = f"plink2 --pfile {geno_path} --extract {temp_snps_path} --export Av --freq --missing --out {plink_out}"
        subprocess.run(extract_cmd, shell=True, check=True)