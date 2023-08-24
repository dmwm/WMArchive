# schema defition for FWJR production documents

fwjr = \
{'meta_data': {'agent_ver': '1.0.14.pre5',
               'fwjr_id': '1-0',
               'crab_id': 'crab',
               'crab_exit_code': 101,
               'host': 'test.fnal.gov',
               # TODO: need to specify other fields: total_cpu, hs06, etc
               'wn_name': "testhost1234.cern.ch",
               'jobtype': "Processing",
               'jobstate': "success", 
               'ts': 1456500229},
 'LFNArray': ['/store/test/blah.root',
              '/store/unmerged/blah.root',
              '/lfn/fallbackfile.root', '/lfn/skipedfile.root'],
 'LFNArrayRef': ['fallbackFiles',
                 'outputLFNs',
                 'lfn',
                 'skippedFiles',
                 'inputLFNs'],
 'PFNArray': ['root://test.ch/blah.root',
              'root://test.ch/blah2.root',
              ],
 'PFNArrayRef': ['inputPFNs', 'outputPFNs', 'pfn'],  # list of keys whose value is referencing fileArray index
 
 'Campaign': 'TestCampaign',
 
 'PrepID': 'TestPrepID',
 
 'steps': [{'name': 'cmsRun1',
             #'analysis': {}, 
             #'cleanup': {},
             #'logs': {},
             'errors': [
                   {
                       "details": "Failed badly.",
                       "type": "Fatal Exception",
                       "exitCode": 8001
                   }
               ],
             'input': [{'catalog': '',
                        'events': 6893,
                        'guid': 'E8099605-8853-E011-A848-0030487A18F2',
                        'input_source_class': 'PoolSource',
                        'input_type': 'primaryFiles',
                        'lfn': 0,
                        'module_label': 'source',
                        'pfn': 0,
                        'runs': [{'lumis': [164, 165],
                                  'runNumber': 160960}]}],
             'output': [{'StageOutCommand': 'rfcp-CERN',
                         'acquisitionEra': 'CMSSW_7_0_0_pre11',
                         'adler32': 'e503b8b9',
                         'applicationName': 'cmsRun',
                         'applicationVersion': 'CMSSW_7_0_0_pre11',
                         'async_dest': '',
                         'branch_hash': 'c1e135af4ac2eb2b803bb6487be2c80f',
                         'catalog': '',
                         'cksum': '2641269665',
                         'configURL': 'https://couch.config.ch5f4811e9ccd63d563cd62572350f0db8',
                         'events': 0,
                         'globalTag': 'GR_R_62_V3::All',
                         'guid': 'ECCFE421-08CB-E511-9F4C-02163E017804',
                         'inputDataset': '/Cosmics/Run2011A-v1/RAW',
                         'inputLFNs': [0],
                         'inputPFNs': [0],
                         'location': '',
                         'merged': False,
                         'module_label': 'ALCARECOStreamDtCalib',
                         'ouput_module_class': 'PoolOutputModule',
                         'outputDataset': '/test/proc/ALCARECO',
                         'outputLFNs': [1],
                         'outputPFNs': [1],
                         'prep_id': '',
                         'processingStr': 'RECOCOSD_TaskChain_Data_pile_up_test',
                         'processingVer': 1,
                         'runs': [{'lumis': [164, 165],
                                   'eventsPerLumi': [100, 150],
                                   'runNumber': 160960}],
                         'size': 647376,
                         'validStatus': 'PRODUCTION',
                         "SEName": "srm-cms.cern.ch",
                         "PNN": "T2_CERN_CH",
                         "GUID": '',
                         "StageOutCommand": "srmv2-lcg"}],
              'performance': {
                  "multicore": {
                  },
                  "storage": {
                    "readAveragekB": 77.8474891246,
                    "readCachePercentageOps": 0.0,
                    'readMBSec': 0.0438598972596,
                    'readMaxMSec': 4832.84,
                    'readNumOps': 97620.0,
                    'readPercentageOps': 1.00032780168,
                    'readTotalMB': 7423.792,    
                    'readTotalSecs': 0.0,
                    'writeTotalMB': 357.624,
                    'writeTotalSecs': 575158.0
                   },
                   "memory": {
                       "PeakValueRss": 0.0,
                       "PeakValueVsize": 0.0
                   },
                   "cpu": {
                       "TotalJobCPU": 0.39894,
                       "AvgEventCPU": -2.0, # for ("-nan")
                       "MaxEventCPU": 0.0,
                       "AvgEventTime": -1.0, # for ("inf")
                       "MinEventCPU": 0.0,
                       "TotalEventCPU": 0.0,
                       "TotalJobTime": 26.4577,
                       "MinEventTime": 0.0,
                       "MaxEventTime": 0.0,
                       'EventThroughput': 0.0,
                       'TotalLoopCPU': 0.0,
                       'TotalInitTime': 0.0,
                       'TotalInitCPU': 0.0,
                       'NumberOfThreads': 0,
                       'NumberOfStreams': 0
                   }
              },
              'site': 'T2_CH_CERN',
              'start': 1454569735,
              'status': 0,
              'WMCMSSWSubprocess': {
                      'startTime': 0.0,
                      'endTime': 0.0,
                      'wallClockTime': 0.0,
                      'userTime': 0.0,
                      'sysTime': 0.0
              },
              'stop': 1454569736}
            ],
'fallbackFiles': [0],
'skippedFiles': [1],
'task': '/workflow_name/RECOCOSD'}
