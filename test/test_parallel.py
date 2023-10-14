"""_"""
import pathlib
import os
from netCDF4 import Dataset
import numpy as np
import subprocess
import shutil
from metgrid_parallel import NamelistFileGenerator, NamelistInfoGetter

class NCFileChecker:
    """_"""

    def __init__(self, file1_filePath, file2_filePath) -> None:
        self.file1_filePath = file1_filePath
        self.file2_filePath = file2_filePath

    def check(self):
        """_"""

        ncFile1 = Dataset(self.file1_filePath)
        ncFile1.set_auto_mask(False)
        ncFile2 = Dataset(self.file2_filePath)
        ncFile2.set_auto_mask(False)

        ncFile1_variablesName = sorted(list(ncFile1.variables.keys()))
        ncFile2_variablesName = sorted(list(ncFile2.variables.keys()))

        assert ncFile1_variablesName == ncFile2_variablesName, 'variablesName is not equal'

        for i in range(len(ncFile1_variablesName)):
            # print('checking {}...'.format(ncFile1_variablesName[i]))

            ncFile1_var = ncFile1.variables[ncFile1_variablesName[i]]
            ncFile2_var = ncFile2.variables[ncFile2_variablesName[i]]

            self.checkVarDataEqual(ncFile1_var, ncFile2_var)
            self.checkVarAttrsEqual(ncFile1_var, ncFile2_var)
            self.checkglobalAttrsEqual(ncFile1, ncFile2)


        ncFile1.close()
        ncFile2.close()

    def checkVarDataEqual(self, var1, var2):
        """_"""
        assert var1.name == var2.name, 'varName: {} != {}'.format(var1.name, var2.name)

        var1_data = var1[:]
        var2_data = var2[:]

        if var1.name == 'Times':
            if not np.array_equal(var1_data, var2_data):
                print(var1.name, 'not equal')
        else:
            if not np.array_equal(var1_data, var2_data, equal_nan=True):
                print(var1.name, 'not equal')

    def checkVarAttrsEqual(self, var1, var2):
        """_"""
        var1_attrsName: list = sorted(var1.ncattrs())
        var2_attrsName: list = sorted(var2.ncattrs())

        assert var1_attrsName == var2_attrsName, 'varAttrsName is not equal'

        for i in range(len(var1_attrsName)):
            attrName = var1_attrsName[i]
            attr1Value = var1.getncattr(attrName)
            attr2Value = var2.getncattr(attrName)

            # check if is numpy array
            if isinstance(attr1Value, (np.ndarray, np.generic)):
                assert np.array_equal(attr1Value, attr2Value)
            else:
                assert attr1Value == attr2Value


    def checkglobalAttrsEqual(self, file1, file2):
        """_"""
        file1_attrsName: list = sorted(file1.ncattrs())
        file2_attrsName: list = sorted(file2.ncattrs())

        assert file1_attrsName == file2_attrsName, 'fileAttrsName is not equal'

        for i in range(len(file1_attrsName)):
            attrName = file1_attrsName[i]
            attr1Value = file1.getncattr(attrName)
            attr2Value = file2.getncattr(attrName)

            if isinstance(attr1Value, (np.ndarray, np.generic)):
                assert np.array_equal(attr1Value, attr2Value)
            else:
                assert attr1Value == attr2Value


def test_metgrid_parallel():
    """_"""

    domainBaseDir = '/root/wrf/domain/testMetgridParallel'
    serialRunDir = os.path.join(domainBaseDir, 'run_serial')
    parallelRunDir = os.path.join(domainBaseDir, 'run_parallel')

    # prepareDir(serialRunDir, parallelRunDir)

    createNamelistForTest(domainBaseDir, serialRunDir, parallelRunDir)
    # linkOtherFilesForTest(domainBaseDir, serialRunDir, parallelRunDir)

    # serial run
    # subprocess.run('/root/wrf/Build_WRF/WPS_Chem_45/metgrid.exe', cwd=serialRunDir)

    # parallel run
    metgridParallelPyFileDir = str(pathlib.Path(__file__).parent.parent.resolve())
    metgridParallelPyFilePath = os.path.join(metgridParallelPyFileDir, 'metgrid_parallel.py')
    pythonExe_path = os.path.join(metgridParallelPyFileDir, '.venv/bin/python')
    metgridExe_path = '/root/wrf/Build_WRF/WPS_Chem_45/metgrid.exe'
    namelist_path = os.path.join(parallelRunDir, 'namelist.wps')
    subprocess.run([pythonExe_path, metgridParallelPyFilePath, '-m', metgridExe_path, '-n', namelist_path], cwd=parallelRunDir)


    checkFiles(serialRunDir, parallelRunDir)
    print('all files are equal.')


def prepareDir(serialRunDir: str, parallelRunDir: str):
    """_"""
    if os.path.exists(serialRunDir):
        shutil.rmtree(serialRunDir)
    os.mkdir(serialRunDir)

    if os.path.exists(parallelRunDir):
        shutil.rmtree(parallelRunDir)
    os.mkdir(parallelRunDir)

def createNamelistForTest(baseDir: str, serialRunDir: str, parallelRunDir: str):
    """_"""
    n_serial = NamelistFileGenerator(os.path.join(baseDir, 'namelist.wps'))
    n_serial.generate(
        os.path.join(serialRunDir, 'namelist.wps'),
        specifiedMetgridOutputDir=serialRunDir
    )

    n_parallel = NamelistFileGenerator(os.path.join(baseDir, 'namelist.wps'))
    n_parallel.generate(
        os.path.join(parallelRunDir, 'namelist.wps'),
        specifiedMetgridOutputDir=parallelRunDir
    )

def linkOtherFilesForTest(baseDir: str, serialRunDir: str, parallelRunDir: str):
    """_"""
    # METGRID.TBL
    os.symlink(os.path.join(baseDir, 'METGRID.TBL'), os.path.join(serialRunDir, 'METGRID.TBL'))
    os.symlink(os.path.join(baseDir, 'METGRID.TBL'), os.path.join(parallelRunDir, 'METGRID.TBL'))

    # geo_em files
    geo_em_filesName = [f for f in os.listdir(baseDir) if f.startswith('geo_em.d') and f.endswith('.nc')]
    for geo_em_fileName in geo_em_filesName:
        os.symlink(os.path.join(baseDir, geo_em_fileName), os.path.join(serialRunDir, geo_em_fileName))
        os.symlink(os.path.join(baseDir, geo_em_fileName), os.path.join(parallelRunDir, geo_em_fileName))


def checkFiles(serialRunDir: str, parallelRunDir: str):
    """_"""
    serial_filesName = sorted([f for f in os.listdir(serialRunDir) if f.startswith('met_em.d')])
    parallel_filesName = sorted([f for f in os.listdir(parallelRunDir) if f.startswith('met_em.d')])

    assert len(serial_filesName) == len(parallel_filesName), 'files number is not equal'

    for i in range(len(serial_filesName)):
        print('checking {}...'.format(serial_filesName[i]))
        serial_filePath = os.path.join(serialRunDir, serial_filesName[i])
        parallel_filePath = os.path.join(parallelRunDir, parallel_filesName[i])

        t = NCFileChecker(serial_filePath, parallel_filePath)
        t.check()

# checkFiles('/root/wrf/domain/testMetgridParallel/run_serial', '/root/wrf/domain/testMetgridParallel/run_parallel')

