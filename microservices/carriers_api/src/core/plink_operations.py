import subprocess
from typing import Optional, List


# command interface
class PlinkCommand:
    def get_command_string(self) -> str:
        """Returns the command string to be executed"""
        pass
    
    def execute(self) -> None:
        """Executes the command"""
        cmd = self.get_command_string()
        subprocess.run(cmd, shell=True, check=True)

# concrete command implementations
class ExtractSnpsCommand(PlinkCommand):
    def __init__(self, pfile: str, snps_file: str, out: str):
        self.pfile = pfile
        self.snps_file = snps_file
        self.out = out
        
    def get_command_string(self) -> str:
        return f"plink2 --pfile {self.pfile} --extract {self.snps_file} --make-pgen --out {self.out}"

class FrequencyCommand(PlinkCommand):
    def __init__(self, pfile: str, out: str):
        self.pfile = pfile
        self.out = out
        
    def get_command_string(self) -> str:
        return f"plink2 --pfile {self.pfile} --freq --out {self.out}"

class SwapAllelesCommand(PlinkCommand):
    def __init__(self, pfile: str, swap_file: str, out: str):
        self.pfile = pfile
        self.swap_file = swap_file
        self.out = out
        
    def get_command_string(self) -> str:
        return f"plink2 --pfile {self.pfile} --a1-allele {self.swap_file} 2 1 --make-pgen --out {self.out}"

class UpdateAllelesCommand(PlinkCommand):
    def __init__(self, pfile: str, update_file: str, out: str):
        self.pfile = pfile
        self.update_file = update_file
        self.out = out
        
    def get_command_string(self) -> str:
        return f"plink2 --pfile {self.pfile} --update-alleles {self.update_file} --make-pgen --out {self.out}"

class ExportCommand(PlinkCommand):
    def __init__(self, pfile: str, out: str, additional_args: Optional[List[str]] = None):
        self.pfile = pfile
        self.out = out
        self.additional_args = additional_args or []
        
    def get_command_string(self) -> str:
        cmd_parts = [
            f"plink2 --pfile {self.pfile}",
            "--export Av",
            "--freq",
            "--missing"
        ]
        cmd_parts.extend(self.additional_args)
        cmd_parts.append(f"--out {self.out}")
        return " ".join(cmd_parts)

class CopyFilesCommand(PlinkCommand):
    def __init__(self, source_prefix: str, target_prefix: str):
        self.source_prefix = source_prefix
        self.target_prefix = target_prefix
        
    def get_command_string(self) -> str:
        return f"cp {self.source_prefix}.pgen {self.target_prefix}.pgen && " \
               f"cp {self.source_prefix}.pvar {self.target_prefix}.pvar && " \
               f"cp {self.source_prefix}.psam {self.target_prefix}.psam"