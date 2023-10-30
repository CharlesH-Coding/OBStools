# --------------::
# from obspy.core import read, Stream, Trace, AttribDict
# import folium
# from branca.element import Figure
# import matplotlib.pyplot as plt
# from scipy.signal import csd
# --------------::
# import obspy
# from obspy import taup
# import re
# import obstools
# from obstools.atacr import DayNoise, TFNoise, EventStream, StaNoise, utils
# import obstools.atacr.plotting as atplot
# from obstools.scripts import comply_calculate, atacr_clean_spectra, atacr_correct_event, atacr_daily_spectra, atacr_download_data, atacr_download_event, atacr_transfer_functions
# --------------::

# from . import tools
# from . import metrics
from .io import *

from obspy.core import UTCDateTime as _UTCDateTime
import glob as _g
import pandas as _pd
import numpy as _np
import pickle as _pkl
from obspy.clients.fdsn import Client as _Client
import datetime as _datetime
import os as _os


def AuditEventFolder(eventsfolder,parseby='SAC',Minmag=6.3,Maxmag=6.7):
        catalog = _get_event_catalog(eventsfolder)
        stations, stations_set = GetStationCatalog()
        cols = stations_set.columns.tolist()

        client = _Client()
        prefix = (catalog['Network'] + '.' + catalog['Station']).tolist()
        for ista in range(len(prefix)):
                if parseby=='SAC':
                        fls = _g.glob(eventsfolder + '/' + prefix[ista] + '/*Z.SAC')
                        files = [fi.split('/')[-1] for fi in fls]
                        evna = [f.split('.SAC')[0][0:f.split('.SAC')[0].rfind('.')] for f in files]
                elif parseby=='pkl':
                        fls = _g.glob(eventsfolder + '/' + prefix[ista] + '/*.pkl')
                        files = [fi.split('/')[-1] for fi in fls]
                        evna = [fi.split('.pkl')[0].split('.sta')[0].split('.day')[0].split(prefix[ista] + '.')[-1] for fi in files]
                mww,depth_km,origin_t,event_meta,averaging = [],[],[],[],[]
                print(str(ista+1) + ' of ' + str(len(prefix)) + ' Sta: ' + prefix[ista] + ', ' + str(len(evna)) + ' files found. Collecting metadata from IRIS..')
                for i in range(len(evna)):
                        ev = evna[i]
                        if _np.char.find(files[i].split('.pkl')[0],'.day')>0:
                                averaging.append('day')
                        else:
                                averaging.append('sta')
                        timedelta = 60
                        cat = client.get_events(starttime=(_UTCDateTime.strptime(str(ev),'%Y.%j.%H.%M') + _datetime.timedelta(minutes=0)).strftime("%Y-%m-%d, %H:%M:%S"), endtime=(_UTCDateTime.strptime(str(ev),'%Y.%j.%H.%M') + _datetime.timedelta(minutes=timedelta)).strftime("%Y-%m-%d, %H:%M:%S"),minmagnitude=Minmag, maxmagnitude=Maxmag)
                        mww.append(cat[0].magnitudes[0].mag)
                        depth_km.append(cat[0].origins[0].depth/1000)
                        origin_t.append(cat[0].origins[0].time)
                        event_meta.append(cat)
                stacat_id = _np.where((stations_set.Station==catalog.iloc[ista].Station) & (stations_set.Network==catalog.iloc[ista].Network))[0][0]
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Magnitude_mw')[0][0]] = mww
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Depth_KM')[0][0]] = depth_km
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Origin')[0][0]] = origin_t
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Metadata')[0][0]] = event_meta
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Averaging')[0][0]] = averaging
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Events')[0][0]] = evna
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='Files')[0][0]] = files
                stations_set.iat[stacat_id,_np.where(stations_set.columns=='n_events')[0][0]] = len(evna)
        catalog = stations_set
        if parseby=='SAC':
                catalog = catalog[cols]
        elif parseby=='pkl':
                cols.append('Averaging')
                catalog = catalog[cols]
        return catalog

def GetStationCatalog():
        current_path = _os.path.dirname(__file__)
        excelfile = current_path + '/Janiszewski_etal_2023_StationList.xlsx'

        stas = [
        '7D.FN07A','7D.FN07C','7D.FN12C','7D.FN14A','7D.FS15B','7D.G03A','7D.G03D','7D.G04D',
        '7D.G34D','7D.J11B','7D.J26C','7D.J41C','7D.J42C','7D.J46C','7D.J59C','7D.M07A','7D.M08A',
        'XO.LA33','XO.LA34','XO.LD40','XO.LD41',
        'XE.CC04','XE.CC05','XE.CC06','XE.CC08','XE.CC11',
        'ZA.B01','ZA.B02','ZA.B04','ZA.B05','ZA.B06',
        'YS.PL33','YS.PL62','YS.PL68',
        ]
        cols = [
        'Station','Network','Latitude (deg)','Longitude (deg)',
        'Experiment','Instrument Design','Seismometer','Environment','Pressure Gauge',
        'Water Depth (m)','Distance from Land (km)','Distance to Plate Boundary (km)','Sediment Thickness (m)',
        'Surface Current (m/s)','Crustal Age (Myr)',
        'Start','End','Deployment Length (days)',
        'Good Channels','n_events','Magnitude_mw','Origin','Metadata','Averaging','Events','Files','Depth_KM']

        stations = _pd.read_excel(excelfile)
        staname = stations.Network.astype(str) + '.' + stations.Station.astype(str)
        allgood = _np.in1d(stations[['Z Is Good','H1 Is Good','H2 Is Good','P Is Good']].sum(axis=1).tolist(),4)
        stations['StaName'] = staname
        stations['Good Channels'] = allgood
        stations = stations.assign(n_events=_pd.Series())
        stations = stations.assign(Magnitude_mw=_pd.Series())
        stations = stations.assign(Depth_KM=_pd.Series())
        stations = stations.assign(Origin=_pd.Series())
        stations = stations.assign(Metadata=_pd.Series())
        stations = stations.assign(Averaging=_pd.Series())
        stations = stations.assign(Events=_pd.Series())
        stations = stations.assign(Files=_pd.Series())
        stations_subset = stations.iloc[_np.where(_np.isin(_np.array(stations['StaName']),stas))]
        stations_subset = stations_subset[cols]
        stations_subset = stations_subset.sort_values(by=['Network','Station'])
        stations_subset = stations_subset.reset_index(drop=True)
        stations = stations.sort_values(by=['Network','Station'])
        stations = stations.reset_index(drop=True)
        return stations,stations_subset

def Get_ATaCR_Dirs(CompFolder):
        ATaCR_Py_DataFolder = dict()
        ATaCR_Py_DataFolder['Py_DataParentFolder'] = CompFolder + '/ATaCR_Python'
        ATaCR_Py_DataFolder['Py_RawDayData'] = ATaCR_Py_DataFolder['Py_DataParentFolder'] + '/Data'
        # ATaCR_Py_DataFolder['Py_PreProcDayData']
        # ATaCR_Py_DataFolder['Py_RawEventData']
        # ATaCR_Py_DataFolder['Py_PreProcEventData']
        ATaCR_Py_DataFolder['Py_StaSpecAvg'] = ATaCR_Py_DataFolder['Py_DataParentFolder'] + '/AVG_STA'
        ATaCR_Py_DataFolder['Py_CorrectedTraces'] = ATaCR_Py_DataFolder['Py_DataParentFolder'] + '/EVENTS'
        ATaCR_Py_DataFolder['Py_b1b2_StaSpectra'] = ATaCR_Py_DataFolder['Py_DataParentFolder'] + '/SPECTRA'
        ATaCR_Py_DataFolder['Py_TransferFunctions'] = ATaCR_Py_DataFolder['Py_DataParentFolder'] + '/TF_STA'
        return ATaCR_Py_DataFolder

def Get_ATaCR_CorrectedEvents(eventfolder,eventnames,net,sta):
        if not isinstance(eventnames,list):
                eventnames = [eventnames]
        if not isinstance(net,list):
                net = [net]
        if not isinstance(sta,list):
                sta = [sta]
        dobspy,raw_collect,corrected = [],[],[]
        for i in range(len(eventnames)):
                neti = net[i]
                stai = sta[i]
                evi = eventnames[i]
                prefix = neti + '.' + stai
                f = eventfolder + '/' + prefix + '/CORRECTED/' + prefix + '.' + evi + '.sta.pkl'
                ri = _pkl.load(open(f,'rb'))
                raw = {'tr1':ri.tr1.copy(),'tr2':ri.tr2.copy(),'trZ':ri.trZ.copy(),'trP':ri.trP.copy()}
                trcorr = {}
                raw_collect.append(raw)
                # rawz.append(ri.trZ.copy())
                dobspy.append(ri)
                for k in list(ri.correct.keys()): #Shape corrected traces into a list of ObsPy trace objects
                        tr = ri.trZ.copy()
                        tr.data = ri.correct[k]
                        tr.stats.location = k
                        trcorr[k] = tr
                corrected.append(trcorr)
        out = _pd.DataFrame({'Event':eventnames,'Network':net,'Station':sta,'Raw':raw_collect,'Corrected':corrected,'Obspy':dobspy})
        return out

def _maxstrfind(s,p):
        '''
        Wild this doesnt exist in Python libraries. Finds LAST occurence of expression in a string.
        '''
        i = 0
        while i>-1:
                io = i
                i = s.find(p,i+1,len(s))
        return io

def _datenum_to_datetime64(dnum):
        '''
        Just some Matlab DateNum nonsense
        '''
        days = _np.asarray(dnum) - 719529  # shift to unix epoch (1970-01-01)
        return _np.round((days * 86400000)).astype("datetime64[ms]")

def _loadpickles(path):
        if isinstance(path,str):
                py_files = [path]
        else:
                py_files = _g.glob(path + '*.pkl')
        if len(py_files)==0:
                raise Exception('Folder contains no .pkl files')
        out = _pd.DataFrame({'Output':[ [] for _ in range(len(py_files)) ],'File':[ [] for _ in range(len(py_files)) ]})
        for i in range(len(py_files)):
                f = py_files[i]
                file = open(f, 'rb')
                pydata = _pkl.load(file)
                out.iloc[i]['Output'] = pydata
                file.close()
        out['File'] = _np.array([a.rsplit('/',1) for a in py_files])[:,1]
        return out

def _get_event_catalog(eventsfolder):
        evdict = dict()
        evdict['folder'] = [fld.split('/')[-2] for fld in _g.glob(eventsfolder + '/*/')]
        evdict['Network'] = [fld.split('.')[0] for fld in evdict['folder']]
        evdict['Station'] = [fld.split('.')[1] for fld in evdict['folder']]
        evdict['folder'][0]
        evdict['n_events'] = list()
        evdict['events'] = list()
        for i in range(len(evdict['folder'])):
                files = [f.split('/')[-1] for f in _g.glob(eventsfolder + '/' + evdict['folder'][i] + '/*.SAC')]
                events = list(_np.unique(['.'.join(files[g].split('.SAC')[0].split('.')[0:-1]) for g in range(len(files))]))
                evdict['events'].append(events)
                evdict['n_events'].append(len(events))
        catalog = _pd.DataFrame(evdict)
        catalog = catalog.sort_values(by=['Network','Station'])
        catalog = catalog.reset_index(drop=True)
        return catalog