from . import io as _io
import pandas as _pd
import numpy as _np
from obspy.core import UTCDateTime as _UTCDateTime
import datetime as _datetime
from obstools.scripts import comply_calculate as _comply_calculate
from obstools.scripts import atacr_clean_spectra as _atacr_clean_spectra
from obstools.scripts import atacr_correct_event as _atacr_correct_event
from obstools.scripts import atacr_daily_spectra as _atacr_daily_spectra
from obstools.scripts import atacr_download_data as _atacr_download_data
from obstools.scripts import atacr_download_event as _atacr_download_event
from obstools.scripts import atacr_transfer_functions as _atacr_transfer_functions
import sys as _sys

def S0_Run_ATaCR(catalog,
        STEPS=[2,3,4,5,6,7],
        netsta_names=None,
        Minmag=6.3, Maxmag=6.7,
        limit=1000,
        pre_event_min_aperture=1, pre_event_day_aperture=30,
        S4_DailySpectra_flags='-O --figQC --figAverage --figCoh --save-fig',
        S5_CleanSpectra_flags='-O --figQC --figAverage --figCoh --figCross --save-fig',
        tf_flags='-O --figTF --save-fig',
        S7_CorrectEvents_flags='--figRaw --figClean --save-fig',
        subfolder=None):

        '''
        A simple wrapper that runs any/all steps, in sequence, of the ATaCR process from start to finish.

        One required argument:
        -
        catalog: A pandas dataframe following the format in the stations list from Janiszewski et al. (2023). The function getstalist() will produce this catalog.
        -

        Optional arguments:

                subfolder: A subfolder to place the logoutputs from each step in ATaCR. By default the logs are places in the ATaCR Data folder.
                netsta_names: Optional list of station names in format ('NET.STA') that user wants to process as opposed to the entire catalog of stations.

                STEPS: A list of the steps in ATaCR that will be run. By default, all steps are run except the first, STEPS=[2,3,4,5,6,7].

                        -STEP 1: Station Metadata

                                Build an stdb file of the stations to be run through ATaCR. Currently I have a function (buid_staquery) that mimics the output from stdb in this step but no longer requires connection to IRIS/FDSN to complete this step.
                                This was important when subsequent steps in ATaCR need to be run but an internet connection wasn't available. ie, downloading events and day noise requires a connection but all subsequent steps in ATaCR shouldn't.

                        -STEP 2: Download Event Data

                                Downloads events that satisfies the criteria defined by the arguments:
                                'limit': Max number of events to get (default, 1000),
                                'Minmag': Min Mw magnitude (default, 6.3),
                                'Maxmag': Max Mw magnitude (default, 6.7),
                                'pre_event_min_aperture': Lead time (default, 1 minute) before arrival to start downloading events. T0 is still corrected but this lead time can be used to help with tapers in filtering or bad clocks on stations,

                        -STEP 3: Download Day Data

                                Downloads day long noise data for the numbers of days before each event time detailed in catalog defined by pre_event_day_aperture (default, 30 days)

                        -STEP 4: Quality Control Noise Data

                                Default argument flags, S4_DailySpectra_flags, for event corrections are:
                                '-O --figQC --figAverage --figCoh --save-fig'.
                                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.

                        -STEP 5: Spectral Average of Noise Data

                                Default argument flags, S5_CleanSpectra_flags, for event corrections are:
                                '-O --figQC --figAverage --figCoh --figCross --save-fig'.
                                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.

                        -STEP 6: Calculate Transfer Functions

                                Default argument flags, tf_flags, for event corrections are:
                                '-O --figTF --save-fig'.
                                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.

                        -STEP 7: Correct Event Data

                                Default argument flags, S7_CorrectEvents_flags, for event corrections are --figRaw --figClean --save-fig.
                                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.

        '''
        if 1 in STEPS:
                print('Step 1/7 - BEGIN: Station Metadata')
                # C='?H?' #channels
                # !query_fdsn_stdb -N {','.join(N)} -C '{C}' -S {','.join(S)} ./Data/sta_query> ./Data/Step_1_7_StationMeta_logfile.log
                _io.S1_Build_StationCatalog(catalog,staquery_output = './Data/sta_query.pkl',subfolder=subfolder)
                print('Step 1/7 - COMPLETE: Station Metadata')
        if 2 in STEPS:
                print('Step 2/7 - BEGIN: Download Event Data')
                _io.S2_DownloadEvents(catalog,netsta_names=netsta_names,Minmag=Minmag,Maxmag=Maxmag,limit=limit,pre_event_min_aperture=pre_event_min_aperture,subfolder=subfolder)
                print('Step 2/7 - COMPLETE: Download Event Data')
        if 3 in STEPS:
                print('Step 3/7 - BEGIN: Download Day Data')
                _io.S3_DownloadNoise(catalog,netsta_names=netsta_names,pre_event_day_aperture=pre_event_day_aperture,subfolder=subfolder)
                print('Step 3/7 - COMPLETE: Download Day Data')

        if 4 in STEPS:
                print('Step 4/7 - BEGIN: Quality Control Noise Data')
                _io.S4_DailySpectra(catalog,netsta_names=netsta_names,extra_flags=S4_DailySpectra_flags,subfolder=subfolder)
                print('Step 4/7 - COMPLETE: Quality Control Noise Data')
        if 5 in STEPS:
                print('Step 5/7 - BEGIN: Spectral Average of Noise Data')
                # !atacr_clean_spectra -O --figQC --figAverage --figCoh --figCross --save-fig --start='{SpecStart}' --end='{SpecEnd}' ./Data/sta_query.pkl> ./Data/Step_5_7_S5_CleanSpectra_logfile.log
                _io.S5_CleanSpectra(catalog,netsta_names=netsta_names,extra_flags=S5_CleanSpectra_flags,subfolder=subfolder)
                print('Step 5/7 - COMPLETE: Spectral Average of Noise Data')
        if 6 in STEPS:
                print('Step 6/7 - BEGIN: Calculate Transfer Functions')
                # !atacr_transfer_functions -O --figTF --save-fig ./Data/sta_query.pkl> ./Data/Step_6_7_CalcTFs_logfile.log
                _io.S6_TransferFunctions(catalog,netsta_names=netsta_names,extra_flags=tf_flags,subfolder=subfolder)
                print('Step 6/7 - COMPLETE: Calculate Transfer Functions')
        if 7 in STEPS:
                print('Step 7/7 - BEGIN: Correct Event Data')
                # !atacr_correct_event --figRaw --figClean --save-fig ./Data/sta_query.pkl> ./Data/Step_7_7_S7_CorrectEvents_logfile.log
                _io.S7_CorrectEvents(catalog,netsta_names=netsta_names,extra_flags=S7_CorrectEvents_flags,subfolder=subfolder)
                print('Step 7/7 - COMPLETE: Correct Event Data')

def S1_Build_StationCatalog(d,fout=None):
        '''STEP 1: Station Metadata
                Build an stdb file of the stations to be run through ATaCR. Currently I have a function (buid_staquery) that mimics the output from stdb in this step but no longer requires connection to IRIS/FDSN to complete this step.
                This was important when subsequent steps in ATaCR need to be run but an internet connection wasn't available.'''
        out = dict()
        for csta in d.iloc:
                net = csta.Network
                sta = csta.Station
                key = net + '.' + sta
                csta_dict = {'station':sta, 'network':net, 'altnet':[],'channel':'*H', 'location':['--'], 'latitude':csta['Latitude (deg)'], 'longitude':csta['Longitude (deg)'], 'elevation':-csta['Water Depth (m)']/1000, 'startdate':UTCDateTime(csta.Start), 'enddate':UTCDateTime(csta.End), 'polarity':1.0, 'azcorr':0.0, 'status':'open'}
                out[key] = csta_dict
        output = _pd.DataFrame.from_dict(out)
        if fout is not None:
                output.to_pickle(fout)
        else:
                return output

def S2_DownloadEvents(catalog,netsta_names=None,Minmag=6.3,Maxmag=6.7,limit=1000,pre_event_min_aperture=1,subfolder=None):
        '''
        STEP 2: Download Event Data
                Downloads events that satisfies the criteria defined by the arguments:
                'limit': Max number of events to get (default, 1000),
                'Minmag': Min Mw magnitude (default, 6.3),
                'Maxmag': Max Mw magnitude (default, 6.7),
                'pre_event_min_aperture': Lead time (default, 1 minute) before arrival to start downloading events. T0 is still corrected but this lead time can be used to help with tapers in filtering or bad clocks on stations,
        '''

        if subfolder is not None:
                logoutput = subfolder + '_Step_2_7_EventDownload_logfile.log'
        else:
                logoutput = '_Step_2_7_EventDownload_logfile.log'
        datafolder = './Data/'
        staquery_output = datafolder + 'sta_query.pkl'
        dateformat = '%Y.%j.%H.%M'
        print('----Begin Event Download----')
        if netsta_names is not None:
                catalog = catalog[_np.in1d((catalog.Network + '.' + catalog.Station),netsta_names)]
        for i in range(len(catalog)):
                csta = catalog.iloc[i]
                S = csta['Station']#station
                N = csta['Network']
                staname = str(N) + '.' + str(S)
                if os.path.isdir(datafolder + staname)==False:
                        os.system('mkdir ' + datafolder + '/' + staname)
                _io.S1_Build_StationCatalog(catalog[(catalog.Network==N) & (catalog.Station==S)],staquery_output)
                log_fout = datafolder + staname + '/' + logoutput
                original = _sys.stdout
                _sys.stdout = open(log_fout,'w+')
                print('--' + staname + '--',flush=True)
                for j in range(len(csta.Events)):
                        print(staname + ' Station ' +str(i+1) + '/' + str(len(catalog)) + ' - Event ' + str(j+1) + '/' + str(len(csta.Events)),flush=True)
                        ev = csta.Events[j]
                        EventStart = _UTCDateTime.strptime(ev,dateformat)
                        EventEnd = _UTCDateTime.strptime(ev,dateformat) + datetime.timedelta(minutes=pre_event_min_aperture)
                        args = [staquery_output,'--start={}'.format(EventStart), '--end={}'.format(EventEnd),'--min-mag={}'.format(Minmag),'--max-mag={}'.format(Maxmag),'--limit={}'.format(limit)]
                        # with open(log_fout, 'w') as _sys.stdout:
                        _atacr_download_event.main(_atacr_download_event.get_event_arguments(args))
        print(' ')
        print('----Event Download Complete----')
        _sys.stdout = original

def S3_DownloadNoise(catalog,netsta_names=None,pre_event_day_aperture=30,subfolder=None):
        '''STEP 3: Download Day Data
                Downloads day long noise data for the numbers of days before each event time detailed in catalog defined by pre_event_day_aperture (default, 30 days)'''

        if subfolder is not None:
                logoutput = subfolder + '_Step_3_7_NoiseDownload_logfile.log'
        else:
                logoutput = '_Step_3_7_NoiseDownload_logfile.log'
        datafolder = './Data/'
        staquery_output = datafolder + 'sta_query.pkl'
        dateformat = '%Y.%j.%H.%M'

        print('----Begin Noise Download----')
        if netsta_names is not None:
                catalog = catalog[_np.in1d((catalog.Network + '.' + catalog.Station),netsta_names)]
        for i in range(len(catalog)):
                csta = catalog.iloc[i]
                S = csta['Station']#station
                N = csta['Network']
                staname = str(N) + '.' + str(S)
                if os.path.isdir(datafolder + staname)==False:
                        os.system('mkdir ' + datafolder + '/' + staname)
                _io.S1_Build_StationCatalog(catalog[(catalog.Network==N) & (catalog.Station==S)],staquery_output)
                log_fout = datafolder + staname + '/' + logoutput
                # with open(log_fout, 'w') as _sys.stdout:
                original = _sys.stdout
                _sys.stdout = open(log_fout,'w+')
                print('--' + staname + '--',flush=True)
                for j in range(len(csta.Events)):
                        print(staname + ' Station ' +str(i+1) + '/' + str(len(catalog)) + ' - Event ' + str(j+1) + '/' + str(len(csta.Events)),flush=True)
                        ev = csta.Events[j]
                        NoiseStart = _UTCDateTime.strptime(ev,dateformat) - datetime.timedelta(days=pre_event_day_aperture)
                        NoiseStart = NoiseStart - datetime.timedelta(hours = NoiseStart.hour, minutes = NoiseStart.minute, seconds=NoiseStart.second) #rounds down to the nearest day
                        NoiseEnd = _UTCDateTime.strptime(ev,dateformat)
                        NoiseEnd = NoiseEnd - datetime.timedelta(hours = NoiseEnd.hour, minutes = NoiseEnd.minute, seconds=NoiseEnd.second) #rounds down to the nearest day
                        args = [staquery_output,'--start={}'.format(NoiseStart), '--end={}'.format(NoiseEnd)]
                        # _sys.stdout.flush()
                        _atacr_download_data.main(_atacr_download_data.get_daylong_arguments(args))
        print(' ')
        print('----Noise Download Complete----')
        _sys.stdout = original

def S4_DailySpectra(catalog,netsta_names=None,extra_flags = '-O --figQC --figAverage --figCoh --save-fig',subfolder=None):
        '''STEP 4: Quality Control Noise Data
                Default argument flags, S4_DailySpectra_flags, for event corrections are:
                '-O --figQC --figAverage --figCoh --save-fig'.
                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.'''

        datafolder = './Data/'
        staquery_output = datafolder + 'sta_query.pkl'
        if subfolder is not None:
                logoutput = subfolder + '_Step_4_7_QCSpectra_logfile.log'
        else:
                logoutput = '_Step_4_7_QCSpectra_logfile.log'
        log_fout = datafolder + logoutput
        SpecStart = catalog.Start.min().strftime("%Y-%m-%d, %H:%M:%S")
        SpecEnd = catalog.End.max().strftime("%Y-%m-%d, %H:%M:%S")
        args = [staquery_output]
        [args.append(flg) for flg in extra_flags.split()]
        [args.append(flg) for flg in ['--start={}'.format(SpecStart),'--end={}'.format(SpecEnd)]]
        # with open(log_fout, 'w') as _sys.stdout:
        original = _sys.stdout
        _sys.stdout = open(log_fout,'w+')
        print('----Begin Daily Spectra----')
        if netsta_names is not None:
                catalog = catalog[_np.in1d((catalog.Network + '.' + catalog.Station),netsta_names)]
        _io.S1_Build_StationCatalog(catalog,staquery_output)
        _atacr_daily_spectra.main(_atacr_daily_spectra.get_dailyspec_arguments(args))
        print(' ')
        print('----Daily Spectra Complete----')
        _sys.stdout = original

def S5_CleanSpectra(catalog,netsta_names=None,extra_flags = '-O --figQC --figAverage --figCoh --figCross --save-fig',subfolder=None):
        '''STEP 5: Spectral Average of Noise Data
                Default argument flags, S5_CleanSpectra_flags, for event corrections are:
                '-O --figQC --figAverage --figCoh --figCross --save-fig'.
                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.'''

        datafolder = './Data/'
        staquery_output = datafolder + 'sta_query.pkl'
        if subfolder is not None:
                logoutput = subfolder + '_Step_5_7_S5_CleanSpectra_logfile.log'
        else:
                logoutput = '_Step_5_7_S5_CleanSpectra_logfile.log'
        log_fout = datafolder + logoutput
        SpecStart = catalog.Start.min().strftime("%Y-%m-%d, %H:%M:%S")
        SpecEnd = catalog.End.max().strftime("%Y-%m-%d, %H:%M:%S")
        args = [staquery_output]
        [args.append(flg) for flg in extra_flags.split()]
        [args.append(flg) for flg in ['--start={}'.format(SpecStart),'--end={}'.format(SpecEnd)]]
        # with open(log_fout, 'w') as _sys.stdout:
        original = _sys.stdout
        _sys.stdout = open(log_fout,'w+')
        print('----Begin Clean Spectra----')
        if netsta_names is not None:
                catalog = catalog[_np.in1d((catalog.Network + '.' + catalog.Station),netsta_names)]
        _io.S1_Build_StationCatalog(catalog,staquery_output)
        _atacr_clean_spectra.main(_atacr_clean_spectra.get_cleanspec_arguments(args))
        print(' ')
        print('----Clean Spectra Complete----')
        _sys.stdout = original

def S6_TransferFunctions(catalog,netsta_names=None,extra_flags = '-O --figTF --save-fig',subfolder=None):
        '''STEP 6: Calculate Transfer Functions
                Default argument flags, tf_flags, for event corrections are:
                '-O --figTF --save-fig'.
                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.'''

        datafolder = './Data/'
        staquery_output = datafolder + 'sta_query.pkl'
        if subfolder is not None:
                logoutput = subfolder + '_Step_6_7_CalcTFs_logfile.log'
        else:
                logoutput = '_Step_6_7_CalcTFs_logfile.log'
        log_fout = datafolder + logoutput
        args = [staquery_output]
        [args.append(flg) for flg in extra_flags.split()]
        # with open(log_fout, 'w') as _sys.stdout:
        original = _sys.stdout
        _sys.stdout = open(log_fout,'w+')
        print('----Begin Building Transfer Functions----')
        if netsta_names is not None:
                catalog = catalog[_np.in1d((catalog.Network + '.' + catalog.Station),netsta_names)]
        _io.S1_Build_StationCatalog(catalog,staquery_output)
        _atacr_transfer_functions.main(_atacr_transfer_functions.get_transfer_arguments(args))

        print(' ')
        print('----Building Transfer Functions Complete----')
        _sys.stdout = original

def S7_CorrectEvents(catalog,netsta_names=None,extra_flags = '--figRaw --figClean --save-fig',subfolder=None):
        '''STEP 7: Correct Event Data
                Default argument flags, S7_CorrectEvents_flags, for event corrections are --figRaw --figClean --save-fig.
                For a list of all flags available for correct events, see the documentation on script atacr_correct_event.'''
        # _sys.stdout.flush()
        datafolder = './Data/'
        staquery_output = datafolder + 'sta_query.pkl'
        if subfolder is not None:
                logoutput = subfolder + '_Step_7_7_S7_CorrectEvents_logfile.log'
        else:
                logoutput = '_Step_7_7_S7_CorrectEvents_logfile.log'
        log_fout = datafolder + logoutput
        args = [staquery_output]
        [args.append(flg) for flg in extra_flags.split()]
        # with open(log_fout, 'w') as _sys.stdout:
        if netsta_names is not None:
                catalog = catalog[_np.in1d((catalog.Network + '.' + catalog.Station),netsta_names)]
        _io.S1_Build_StationCatalog(catalog,staquery_output)
        original = _sys.stdout
        _sys.stdout = open(log_fout,'w+')
        print('----Begin Correcting Eventss----')
        _atacr_correct_event.main(_atacr_correct_event.get_correct_arguments(args))
        print(' ')
        print('----Correcting Events Complete----')
        _sys.stdout = original