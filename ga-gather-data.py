import sys
import subprocess
import csv

if len(sys.argv) > 1:
    out_file_path = sys.argv[1]
else:
    raise SyntaxError("Missing output file name")

TEST_CMD = ["printf", "hi:hello\nbye:farewell\ngreet:howdy"]
if len(sys.argv) > 2:
    test_mode = sys.argv[2] == "-t"
else:
    test_mode = False

def pipedPrint(*args):
    print(*args)
    sys.stdout.flush()

def makeDataTuple(str_pair):
    first = str_pair[0]
    second = str_pair[1]
    try:
        second = float(second)
    except:
        pass
    return [first, second]

def exec(cmd, timeout=60, quiet=False):
    if not quiet:
        pipedPrint("Executing:", ' '.join(cmd))
    
    if test_mode:
        cmd = TEST_CMD

    try:
        subprocess.check_output(cmd, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False
    return True

def execToDict(cmd, timeout=60, quiet=False):
    if not quiet:
        pipedPrint("Executing:", ' '.join(cmd))
    
    if test_mode:
        cmd = TEST_CMD

    try:
        lines = subprocess.check_output(cmd, timeout=timeout).decode(sys.stdout.encoding).split('\n')
    except subprocess.TimeoutExpired:
        return None
    
    return dict(makeDataTuple(line.strip().split(":")) for line in lines if line!='')

class Config:
    def __init__(self, options, info):
        self.options = options
        self.info = info

class RunCollect:
    def __init__(self, app, appOption, graphPath, graphs):
        self.app = app
        self.appOption = appOption
        self.graphPath = graphPath
        self.graphs = graphs
        self.configs = []
        self._runDicts = []
        self._dict_prune = None
    
    def addConfig(self, config):
        self.configs.append(config)

    def addConfigs(self, configs):
        self.configs += configs

    def run(self, exec, timeout=60, quiet=False):
        for graph in self.graphs:
            for config in self.configs:
                cmd  = [exec, self.graphPath + graph, self.appOption] + config.options
                
                configDict = {}
                configDict["app"] = self.app
                configDict["graph"] = graph
                configDict["command"] = ' '.join(cmd)
                
                resultDict = None
                while resultDict == None:
                    resultDict = execToDict(cmd, timeout=timeout, quiet=quiet)
                    if not resultDict and not quiet:
                        pipedPrint("\tTimed-out, retrying...")

                if self._dict_prune:
                    self._dict_prune(resultDict)

                runDict = {**configDict, **config.info, **resultDict}
                self._runDicts.append(runDict)

    def setResultPruneFn(self, fn):
        self._dict_prune = fn

    #order results for prop
    def getResults(self):
        return self._runDicts


EXEC = "./graph_analytics"
BASE_GRAPH_PATH = "/homes/mattagostini/graphs/BINARY"
BFS_GRAPH_PATH = BASE_GRAPH_PATH + "/bfs/"
SSSP_GRAPH_PATH = BASE_GRAPH_PATH + "/sssp/"
PR_GRAPH_PATH = BASE_GRAPH_PATH + "/pr/"

BASE_OPTIONS = ["-b", "-q", "-script"]
FPGA_CONFIG = Config(BASE_OPTIONS + [ "-fpga" ], {"processor": "fpga"})
CPU_CONFIG = Config(BASE_OPTIONS + [ "-cpu" ], {"processor": "cpu"})

BFS_GRAPHS = ["as-skitter_bfs.b", "d_s23_e32_bfs.b", "d_s24_e32_bfs.b", "d_s25_e32_bfs.b", "soc-LiveJournal1_bfs.b", "twitter_bfs.b"]
SSSP_GRAPHS = ["as-skitter_sssp.b", "d_s23_e32_sssp.b", "d_s24_e32_sssp.b", "d_s25_e32_sssp.b", "soc-LiveJournal1_sssp.b", "twitter_sssp.b"]
PR_GRAPHS = ["as-skitter_pr.b", "d_s23_e32_pr.b", "d_s24_e32_pr.b", "d_s25_e32_pr.b", "soc-LiveJournal1_pr.b", "twitter_pr.b"]

BFS_BITSTREAM = "/homes/obrienfr/bitstreams/bfs/ws_bfs_296.gbs"
SSSP_BITSTREAM = "/homes/obrienfr/bitstreams/sssp/ws_sssp_286.gbs"
PR_BITSTREAM = "/homes/obrienfr/bitstreams/pr/ws_pr_292.gbs"

def pruneUnecessary(dict):
    dict.pop("gthr_overhead_ms", None)
    dict.pop("sctr_overhead_ms", None)
    dict.pop("is_workstealing", None)
    dict.pop("gthr_lambda", None)
    dict.pop("sctr_lambda", None) 

bfs_run = RunCollect("bfs", "-bfs", BFS_GRAPH_PATH, BFS_GRAPHS)
bfs_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
bfs_run.setResultPruneFn(pruneUnecessary)

sssp_run = RunCollect("sssp", "-sssp", SSSP_GRAPH_PATH, SSSP_GRAPHS)
sssp_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
sssp_run.setResultPruneFn(pruneUnecessary)

pr_run = RunCollect("pr", "-pr", PR_GRAPH_PATH, PR_GRAPHS)
pr_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
pr_run.setResultPruneFn(pruneUnecessary)

exec(["fpgaconf", BFS_BITSTREAM])
bfs_run.run("./graph_analytics", timeout=300)

exec(["fpgaconf", SSSP_BITSTREAM])
sssp_run.run("./graph_analytics", timeout=300)

exec(["fpgaconf", PR_BITSTREAM])
pr_run.run("./graph_analytics", timeout=300)

results = bfs_run.getResults() + sssp_run.getResults() + pr_run.getResults()

with open(out_file_path, 'w+') as output_file:
    dict_writer = csv.DictWriter(output_file, results[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(results)