import json
import json
fwjr = \
{'meta_data': {'agent_ver': '1.0.14.pre5',
               'fwjr_id': '1-0',
               'host': 'test.fnal.gov',
               'ts': 1456500229},
 'fileArray': ['/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root',
               'root://eoscms.cern.ch//eos/cms/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root',
               '/store/unmerged/CMSSW_7_0_0_pre11/Cosmics/ALCARECO/DtCalib-RECOCOSD_TaskChain_Data_pile_up_test-v1/00000/ECCFE421-08CB-E511-9F4C-02163E017804.root',
               'root://eoscms.cern.ch//eos/cms/store/unmerged/CMSSW_7_0_0_pre11/Cosmics/ALCARECO/DtCalib-RECOCOSD_TaskChain_Data_pile_up_test-v1/00000/ECCFE421-08CB-E511-9F4C-02163E017804.root',
               '/lfn/fallbackfile.root', '/lfn/skipedfile.root'],
 'fileArrayRef': ['lfn', 'pfn', 'inputLFNs', 'inputPFNs', 
                  'outputLFNs', 'outputPFNs', 'fallbackFiles', 'skippedFiles'], # list of keys whose value is referencing fileArray index
 'steps': [{'name': 'cmsRun1',
             'analysis': {},
             'cleanup': {},
             'logs': {},
             'errors': [
                   {
                       "details": "An exception of category 'ExternalLHEProducer' occurred while\n   [0] Processing run: 1\n   [1] Running path 'lhe_step'\n   [2] Calling beginRun for module ExternalLHEProducer/'externalLHEProducer'\nException Message:\nChild failed with exit code 2.",
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
                        'pfn': 1,
                        'runs': [{'lumis': [164, 165],
                                  'runNumber': 160960}]}],
             'output': [{'StageOutCommand': 'rfcp-CERN',
                         'acquisitionEra': 'CMSSW_7_0_0_pre11',
                         'adler32': 'e503b8b9',
                         'applicationName': 'cmsRun',
                         'applicationVersion': 'CMSSW_7_0_0_pre11',
                         #TODO change to empty string
                         'async_dest': '',
                         'branch_hash': 'c1e135af4ac2eb2b803bb6487be2c80f',
                         'catalog': '',
                         'cksum': '2641269665',
                         'configURL': 'https://cmsweb.cern.ch/couchdb;;reqmgr_config_cache;;5f4811e9ccd63d563cd62572350f0db8',
                         'events': 0,
                         'globalTag': 'GR_R_62_V3::All',
                         'guid': 'ECCFE421-08CB-E511-9F4C-02163E017804',
                         'inputDataset': '/Cosmics/Run2011A-v1/RAW',
                         'inputLFNs': [0],
                         'inputPFNs': [1],
                         #TODO change to empty string from None
                         'location': '',
                         'merged': False,
                         'module_label': 'ALCARECOStreamDtCalib',
                         'ouput_module_class': 'PoolOutputModule',
                         'outputDataset': '/Cosmics/CMSSW_7_0_0_pre11-DtCalib-RECOCOSD_TaskChain_Data_pile_up_test-v1/ALCARECO',
                         #TODO need to change thsi to list format
                         'outputLFNs': [2],
                         'outputPFNs': [3],
                         #TODO change to empty string from 'None'
                         'prep_id': '',
                         'processingStr': 'RECOCOSD_TaskChain_Data_pile_up_test',
                         'processingVer': 1,
                         'runs': [{'lumis': [164, 165],
                                   'runNumber': 160960}],
                         'size': 647376,
                         #TODO remove this 
                         #'user_dn': '',
                         #'user_vogroup': 'DEFAULT',
                         #'user_vorole': 'DEFAULT',
                         'validStatus': 'PRODUCTION'}],
              'performance': {"storage": {},
                   "multicore": {},
                   "memory": {
                       "PeakValueRss": 0,
                       "PeakValueVsize": 0
                   },
                   "cpu": {
                       "TotalJobCPU": 0.39894,
                       "AvgEventCPU": -2.0, # for ("-nan")
                       "MaxEventCPU": 0,
                       "AvgEventTime": -1.0, # for ("inf")
                       "MinEventCPU": 0,
                       "TotalEventCPU": 0,
                       "TotalJobTime": 26.4577,
                       "MinEventTime": 0.0,
                       "MaxEventTime": 0.0
                   }},
              'site': 'T2_CH_CERN',
              'start': 1454569735,
              'status': 0,
              'stop': 1454569736}
            ],
'fallbackFiles': [4],
'skippedFiles': [5],
'task': '/sryu_TaskChain_Data_wq_testt_160204_061048_5587/RECOCOSD'}

with open("fwjr_test.json", 'w') as outfile:
    json.dump(fwjr, outfile)
    
print "done"