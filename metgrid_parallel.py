""" 此文件使用多核加速WPS中metgrid.exe程序的运行 """
import argparse
import subprocess
import os
import datetime
from multiprocessing import Pool
# import pathos.pools as pp
from util import all_equal, getPhysicalCPUCoreNum

def execMetgridInShell(metgridFilePath: str, dirWhereMetgridRun: str):
    subprocess.run([metgridFilePath], cwd=dirWhereMetgridRun)

class Metgrid_runner:
    """_"""
    def __init__(self):
        self.metgridFilePath, self.namelistFilePath, self.specifiedMetgridOutputDir = Arguments_getter().get_args()
        print(self.metgridFilePath, self.namelistFilePath)
        self.metgridTBLFileName = 'METGRID.TBL'

    def run_metgrid(self):
        """_"""
        namelistInfoGetter = NamelistInfoGetter(self.namelistFilePath)
        startDate, endDate, FILETimeInterval, domainNum = namelistInfoGetter.getDateTimeInfo()

        parallelRunTime = self.calculateParallelRunTime(startDate, endDate, FILETimeInterval)

        parallelRunFolderPath = self.createParallelRunFolder()

        self.prepareParallelRunSubFolder(parallelRunFolderPath, startDate, FILETimeInterval, parallelRunTime, domainNum)

        # exec shell command
        p = Pool(getPhysicalCPUCoreNum())

        for i in range(parallelRunTime):
            runDir = self.getParallelRunFolderPath(parallelRunFolderPath, i)
            p.apply_async(execMetgridInShell, args=(self.metgridFilePath, runDir))

        print('Waiting for all subprocesses done...')
        p.close()
        p.join()

    def getParallelRunFolderPath(self, parallelRunFolderPath: str, work_index: int):
        """_"""
        return os.path.join(parallelRunFolderPath, 'parallelRun_{:03d}'.format(work_index))

    def prepareParallelRunSubFolder(self, parallelRunFolderPath: str, startDate: datetime.datetime, FILETimeInterval, parallelRunTime: int, domainNum: int):
        """_"""
        namelistFileGenerator = NamelistFileGenerator(self.namelistFilePath)

        for i in range(parallelRunTime):
            runDir = self.getParallelRunFolderPath(parallelRunFolderPath, i)
            os.mkdir(runDir)

            # TBL file symlink
            namelistFileDir = os.path.dirname(self.namelistFilePath)
            os.symlink(os.path.join(namelistFileDir, self.metgridTBLFileName), os.path.join(runDir, self.metgridTBLFileName))

            # geo_em file symlink
            for domainIndex in range(domainNum):
                os.symlink(os.path.join(namelistFileDir, 'geo_em.d{:02d}.nc'.format(domainIndex + 1)), os.path.join(runDir, 'geo_em.d{:02d}.nc'.format(domainIndex + 1)))
                pass

            # generate namelist.wps file
            currentDateTime = startDate + i * FILETimeInterval
            namelistFileGenerator.generate(os.path.join(runDir, 'namelist.wps'), currentDateTime, currentDateTime, self.specifiedMetgridOutputDir)


    def calculateParallelRunTime(self, startDate: datetime.datetime, endDate: datetime.datetime, FILETimeInterval: datetime.timedelta):
        """_"""
        assert endDate >= startDate, 'end_date must be later than start_date'
        assert (endDate - startDate).total_seconds() % FILETimeInterval.total_seconds() == 0, 'end_date - start_date must be a multiple of interval_seconds'

        return (int((endDate - startDate).total_seconds()) // int(FILETimeInterval.total_seconds())) + 1

    def createParallelRunFolder(self) -> str:
        """_"""
        namelistFileDir = os.path.dirname(self.namelistFilePath)
        # note that nowStr may not correspond to the actual time
        # because the timezone of your computer may not be set correctly
        nowStr = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        parallelRunFolderName = 'parallelRun_{}'.format(nowStr)
        print(parallelRunFolderName)
        parallelRunFolderPath = os.path.join(namelistFileDir, parallelRunFolderName)
        os.mkdir(parallelRunFolderPath)

        return parallelRunFolderPath

class NamelistFileGenerator:
    """_"""
    def __init__(self, inputNamelistFilePath: str) -> None:
        self.namelistFilePath = inputNamelistFilePath

        with open(self.namelistFilePath, 'r') as namelistFile:
            self.namelistLines = namelistFile.readlines()

        _, self.domainNum = NamelistInfoGetter.getStartDate_dateTime(self.namelistLines)

    def generate(self, outputNamelistFilePath: str, startDate=None, endDate=None, specifiedMetgridOutputDir=None):
        """_"""
        namelistLines_copy = self.namelistLines.copy()

        if startDate is not None:
            startDateLineIndex = NamelistInfoGetter.getStartDateLineIndex(self.namelistLines)
            startDate_leadingWhiteSpaces = self.getLeadingWhiteSpaces(self.namelistLines[startDateLineIndex])
            namelistLines_copy[startDateLineIndex] = self.generateStartDateLine(startDate, startDate_leadingWhiteSpaces)

        if endDate is not None:
            endDateLineIndex = NamelistInfoGetter.getEndDateLineIndex(self.namelistLines)
            endDate_leadingWhiteSpaces = self.getLeadingWhiteSpaces(self.namelistLines[endDateLineIndex])
            namelistLines_copy[endDateLineIndex] = self.generateEndDateLine(endDate, endDate_leadingWhiteSpaces)

        if specifiedMetgridOutputDir is None:
            if not NamelistInfoGetter.metgridOutputDirIsAbsolute(self.namelistLines):
                metgridOutputDirLineIndex = NamelistInfoGetter.getMetgridOutputDirLineIndex(self.namelistLines)
                metgridOutputDir_leadingWhiteSpaces = self.getLeadingWhiteSpaces(self.namelistLines[metgridOutputDirLineIndex])
                metgridOutputAbsoluteDir = NamelistInfoGetter.getMetgridOutputAbsoluteDir(self.namelistLines, self.namelistFilePath)
                namelistLines_copy[metgridOutputDirLineIndex] = self.generateMetgridOutputDirLine(metgridOutputDir_leadingWhiteSpaces, metgridOutputAbsoluteDir)
        else:
            metgridOutputDirLineIndex = NamelistInfoGetter.getMetgridOutputDirLineIndex(self.namelistLines)
            metgridOutputDir_leadingWhiteSpaces = self.getLeadingWhiteSpaces(self.namelistLines[metgridOutputDirLineIndex])
            namelistLines_copy[metgridOutputDirLineIndex] = self.generateMetgridOutputDirLine(metgridOutputDir_leadingWhiteSpaces, specifiedMetgridOutputDir)

        self.writeToOutputFile(outputNamelistFilePath, namelistLines_copy)

    def writeToOutputFile(self, outputNamelistFilePath: str, namelistLines_copy: list):
        with open(outputNamelistFilePath, 'w') as outputNamelistFile:
            outputNamelistFile.writelines(namelistLines_copy)

    def generateStartDateLine(self, startDate: datetime.datetime, startDate_leadingWhiteSpaces: str):
        """_"""
        startDate_value = ' \'{}\','.format(startDate.strftime('%Y-%m-%d_%H:%M:%S')) * self.domainNum
        startDateLine = startDate_leadingWhiteSpaces + 'start_date ={}\n'.format(startDate_value)
        return startDateLine

    def generateEndDateLine(self, endDate: datetime.datetime, endDate_leadingWhiteSpaces: str):
        """_"""
        endDate_value = ' \'{}\','.format(endDate.strftime('%Y-%m-%d_%H:%M:%S')) * self.domainNum
        endDateLine = endDate_leadingWhiteSpaces + 'end_date ={}\n'.format(endDate_value)
        return endDateLine

    def generateMetgridOutputDirLine(self, metgridOutputDir_leadingWhiteSpaces: str, metgridOutputDir: str):
        """_"""
        return metgridOutputDir_leadingWhiteSpaces + 'opt_output_from_metgrid_path = \'{}\',\n'.format(metgridOutputDir)

    def getLeadingWhiteSpaces(self, line: str):
        """_"""
        return line[:len(line) - len(line.lstrip())]

class NamelistInfoGetter:
    """_"""

    def __init__(self, namelistFilePath: str) -> None:
        self.namelistFilePath = namelistFilePath

    def getDateTimeInfo(self):
        """_"""
        try:
            with open(self.namelistFilePath, 'r') as namelistFile:
                namelistLines = namelistFile.readlines()
        except FileNotFoundError:
            print('No such file or directory: {}\nPlease check namelist.wps file path settings.\nAbort.'.format(self.namelistFilePath))
            exit(1)


        startDate, domainNum_startDate = NamelistInfoGetter.getStartDate_dateTime(namelistLines)
        endDate, domainNum_endDate = NamelistInfoGetter.getEndDate_dateTime(namelistLines)
        assert domainNum_startDate == domainNum_endDate, 'start_date and end_date must have the same number of elements'
        print('start date: {}'.format(startDate))
        print('end date: {}'.format(endDate))
        FILETimeInterval = NamelistInfoGetter.getFILETimeInterval(namelistLines)

        return startDate, endDate, FILETimeInterval, domainNum_startDate

    @staticmethod
    def getStartDateLineIndex(fileLines: list):
        """_"""
        for lineIndex, line in enumerate(fileLines):
            if line.strip().startswith('start_date'):
                return lineIndex
        raise Exception('did not find \'start_date\' in namelist.wps')

    @staticmethod
    def getEndDateLineIndex(fileLines: list):
        """_"""
        for lineIndex, line in enumerate(fileLines):
            if line.strip().startswith('end_date'):
                return lineIndex
        raise Exception('did not find \'end_date\' in namelist.wps')

    @staticmethod
    def getIntervalSecondsLineIndex(fileLines: list):
        """_"""
        for lineIndex, line in enumerate(fileLines):
            if line.strip().startswith('interval_seconds'):
                return lineIndex
        raise Exception('did not find \'interval_seconds\' in namelist.wps')

    @staticmethod
    def getMetgridOutputDirLineIndex(fileLines: list):
        """_"""
        for lineIndex, line in enumerate(fileLines):
            if line.strip().startswith('opt_output_from_metgrid_path'):
                return lineIndex
        raise Exception('did not find \'opt_output_from_metgrid_path\' in namelist.wps')

    @staticmethod
    def getStartDate_dateTime(namelistLines: list):
        """_"""
        startDateLineIndex = NamelistInfoGetter.getStartDateLineIndex(namelistLines)
        startDateLine = namelistLines[startDateLineIndex]

        assert len(startDateLine.split('=')) == 2, 'start_date line is not valid'
        startDates = [startDate.strip() for startDate in startDateLine.split('=')[1].strip().split(',') if startDate.strip() != '']

        assert all_equal(startDates), 'This program only support start_date with the same value for all domains'
        startDate_dateTime = datetime.datetime.strptime(startDates[0], '\'%Y-%m-%d_%H:%M:%S\'')

        return startDate_dateTime, len(startDates)

    @staticmethod
    def getEndDate_dateTime(namelistLines: list):
        """_"""
        endDateLineIndex = NamelistInfoGetter.getEndDateLineIndex(namelistLines)
        endDateLine = namelistLines[endDateLineIndex]

        assert len(endDateLine.split('=')) == 2, 'end_date line is not valid'
        endDates = [endDate.strip() for endDate in endDateLine.split('=')[1].strip().split(',') if endDate.strip() != '']

        assert all_equal(endDates)
        endDate_dateTime = datetime.datetime.strptime(endDates[0], '\'%Y-%m-%d_%H:%M:%S\'')

        return endDate_dateTime, len(endDates)

    @staticmethod
    def getFILETimeInterval(namelistLines: list):
        """_"""
        intervalSecondsLineIndex = NamelistInfoGetter.getIntervalSecondsLineIndex(namelistLines)
        intervalSecondsLine = namelistLines[intervalSecondsLineIndex]

        assert len(intervalSecondsLine.split('=')) == 2, 'interval_seconds line is not valid'
        timeIntervalStr = intervalSecondsLine.split('=')[1].strip().split(',')[0].strip()

        return datetime.timedelta(seconds=int(timeIntervalStr))

    @staticmethod
    def getMetgridOutputDir(fileLines: list):
        """_"""
        metgridOutputDirLineIndex = NamelistInfoGetter.getMetgridOutputDirLineIndex(fileLines)
        metgridOutputDirLine = fileLines[metgridOutputDirLineIndex]

        assert len(metgridOutputDirLine.split('=')) == 2, 'opt_output_from_metgrid_path line is not valid'
        outputDirs = [outputDir.strip() for outputDir in metgridOutputDirLine.split('=')[1].strip().split(',') if outputDir.strip() != '']
        assert len(outputDirs) == 1, 'opt_output_from_metgrid_path must have only one element'
        assert outputDirs[0].startswith('\'') and outputDirs[0].endswith('\''), 'opt_output_from_metgrid_path must be surrounded by \''

        outputDir = outputDirs[0].strip('\'').strip()

        return outputDir

    @staticmethod
    def metgridOutputDirIsAbsolute(fileLines: list):
        """_"""
        outputDir = NamelistInfoGetter.getMetgridOutputDir(fileLines)
        return os.path.isabs(outputDir)

    @staticmethod
    def getMetgridOutputAbsoluteDir(fileLines: list, namelistFilePath: str):
        """_"""
        outputDir = NamelistInfoGetter.getMetgridOutputDir(fileLines)

        if not os.path.isabs(outputDir):
            namelistFileDir = os.path.dirname(namelistFilePath)
            outputDir = os.path.join(namelistFileDir, outputDir)

        return outputDir


class Arguments_getter:
    """_"""
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='metgrid.exe parallel runner')

    def get_args(self):
        """_"""
        self.parser.add_argument('-m', '--metgridExeFilePath', type=str, help='metgrid.exe file path', required=True)
        self.parser.add_argument('-n', '--namelist', type=str, help='namelist.wps file path, default is current directory', default=os.path.join(os.getcwd(), 'namelist.wps'))
        self.parser.add_argument('-o', '--outputDir', type=str, help='geo_em file output directory, default is the dir set in namelist.wps', default=None)
        args = self.parser.parse_args()

        if args.outputDir is not None and not os.path.isabs(args.outputDir):
            print('outputDir must be an absolute path.\nAbort.')
            exit(1)

        return args.metgridExeFilePath, args.namelist, args.outputDir

if __name__ == '__main__':
    metgridRunner = Metgrid_runner()
    metgridRunner.run_metgrid()

