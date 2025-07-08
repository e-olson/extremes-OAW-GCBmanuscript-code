import sys
import os
import shutil
import glob
import warnings
import xarray as xr
import numpy as np
import datetime as dt
import re
import cftime
import functools
import dask
import dask.array as da
from dask.distributed import Client, LocalCluster
from collections import OrderedDict
warnings.filterwarnings(action='ignore',message='Mean of empty slice',category=RuntimeWarning)
warnings.filterwarnings(action='ignore',message='invalid value encountered in scalar divide',
                        category=RuntimeWarning)
warnings.filterwarnings(action='ignore',message='The specified chunks separate the stored chunks along dimension',
                        category=UserWarning)
dask.config.set({'distributed.worker.memory.target':False,
                 'distributed.worker.memory.spill':False,
                 'distributed.worker.memory.pause':False,
                 'distributed.worker.memory.terminate':False,})

### definitions ##################################################################
sourcepath=' '
workpath=''
tmpdir=''
chunksdef={'time':-1,'lon':180,'lat':90}#{'time':-1,'lon':60,'lat':30}#chunksdef={'time':40*365,'lon':60,'lat':30}
quantiledef=np.array([0,0.02,0.05,0.07,0.10,0.20,0.50,0.80,0.90,0.93,0.95,0.98,1])
targettime_MMM_NOAA=dt.datetime(1988,1,1,0)+dt.timedelta(days=.286*365)
# set defaults to midpoint of slices:
targettime_hist=dt.datetime(1995,1,1,0)
targettime_future=dt.datetime(2081,1,1,0)
yrspan_future=[2061,2100]
scenDefaults={'ESM4_historical_D1':([1975,2014],targettime_hist),
            'ESM4_ssp126_D1':(yrspan_future,targettime_future),
            'ESM4_ssp370_D1':(yrspan_future,targettime_future),}
dictScenDates={ii:scenDefaults[ii][0] for ii in scenDefaults}
yrspanFull={'ESM4_historical_D1':[1850,2014], 
            'ESM4_ssp126_D1':[2015,2100],
            'ESM4_ssp370_D1':[2015,2100],}
# variable information
exdirs={'tos':1,
       'hplusos':1,
       'phos':-1,
       'omega_arag_0':-1,
       'spco2':1,}
dirstr={1:'max',-1:'min'}

fex=f'{sourcepath}ESGF/ESM4_historical_D1/tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_19700101-19791231.nc'
with xr.open_dataset(fex) as fstat:
    lonvec1x1=fstat['lon'].values
    latvec1x1=fstat['lat'].values
    
fstatic1x1=f'{sourcepath}ge_data/ESM4_historical_D1/ocean_daily_1x1deg.static.nc'
with xr.open_dataset(fstatic1x1) as fstat:
    glon1x1=fstat['geolon'].values
    glat1x1=fstat['geolat'].values
    deptho1x1=fstat['deptho'].values
    wet1x1=np.where(fstat['wet'].to_masked_array().mask,0.0,fstat['wet'].values)
    areacello1x1=fstat['areacello'].values

### useful fxns ####################################################################
def _add_dims(arr,tarr):
    while len(np.shape(arr))<len(np.shape(tarr)):
        arr=np.expand_dims(arr,-1)
    return arr
    
def nchasvar(ivar,fname):
    with xr.open_dataset(fname) as ff:
        s=ivar in ff.keys()
    return s

def mkdirs(fsave):
    saveloc=os.path.dirname(fsave)
    if not os.path.exists(saveloc):
        try:
            os.makedirs(saveloc)
        except FileExistsError:
            pass # in case other code running at the same time got to it first
    return

def ydfromdt(dts):
    if isinstance(dts,dt.datetime):
        return (dts-dt.datetime(dts.year-1,12,31)).days
    elif hasattr(dts,'__len__') and ~isinstance(dts,str): # assume array of datetimes
        return np.array([ydfromdt(el) for el in dts])
    elif pd.isnull(dts):
        return np.nan
    else:
        raise TypeError('bad type: ', type(dts))

def exactydfromdt(dts):
    if isinstance(dts,dt.datetime):
        return (dts-dt.datetime(dts.year-1,12,31)).total_seconds()/(24*3600)
    elif hasattr(dts,'__len__') and ~isinstance(dts,str): # assume array of datetimes
        return np.array([ydfromdt(el) for el in dts])
    elif pd.isnull(dts):
        return np.nan
    else:
        raise TypeError('bad type: ', type(dts))

def monthfromdt(dts):
    if isinstance(dts,dt.datetime):
        return dts.month
    else: # assume array of datetimes
        return np.array([monthfromdt(el) for el in dts])

def dt_to_cftnoleap(x):
    if isinstance(x,dt.datetime):
        if x.month==2 and x.day==29:
            return cftime.datetime(x.year,x.month,x.day-1,x.hour,x.minute,x.second,calendar='noleap')
        else:
            return cftime.datetime(x.year,x.month,x.day,x.hour,x.minute,x.second,calendar='noleap')
    else: # assume array
        return np.array([dt_to_cftnoleap(el) for el in x])

def isNoLeap(dts):
    if isinstance(dts,cftime.datetime):
        return dts.calendar=='noleap'
    try:
        if len(dts)>=1:
            return isinstance(dts[0],cftime.datetime) and dts[0].calendar=='noleap'
    except TypeError:
        return False

def ydNLfromcftNL(dts):
    if isNoLeap(dts):
        if type(dts)==list or type(dts)==np.ndarray or type(dts)==np.ma.core.MaskedArray:
            return np.array([ydNLfromcftNL(el) for el in dts])
        else:
            return (dts-cftime.datetime(dts.year-1,12,31,calendar='noleap')).days
    else:
        raise TypeError('input not cftime noleap')

def exactydNLfromcftNL(dts):
    if isNoLeap(dts):
        if type(dts)==list or type(dts)==np.ndarray or type(dts)==np.ma.core.MaskedArray:
            return np.array([ydNLfromcftNL(el) for el in dts])
        else:
            return (dts-cftime.datetime(dts.year-1,12,31,calendar='noleap')).total_seconds()/(24*3600)
    else:
        raise TypeError('input not cftime noleap')

def to_int_tind(ti,freq='daily',torig=dt.datetime(1900,1,1),calendar='infer'):
    if hasattr(ti,'__len__') and ~isinstance(ti,str): # assume array of datetimes
        return np.array([to_int_tind(el,freq=freq,torig=torig,calendar=calendar) for el in ti],dtype='object')
    else:
        return inttimeind(ti,freq=freq,torig=torig,calendar=calendar)

class exacttimeind(np.float64):
    #note: does not pickle/unpickle correctly
    def __new__(cls,ti,torig=dt.datetime(1900,1,1),calendar='infer',fixval=False):
        if calendar=='infer':
            calendar='noleap' if isNoLeap(ti) else 'standard'
        elif calendar=='noleap' or calendar=='standard':
            if ((calendar=='standard' and isNoLeap(ti)) or (calendar=='noleap' and not isNoLeap(ti))) and not fixval:
                raise TypeError('Wrong calendar')
        else:
            raise ValueError('calendar can be infer or standard or noleap')
        if calendar=='noleap':
            torig=cftime.datetime(torig.year,torig.month,torig.day,calendar='noleap')
        if fixval: # for setting attributes on calc'd val
            ival=ti
        else: # normal behavior
            ival=(ti-torig).total_seconds()/(24*3600)
        #ival=(ti-torig).total_seconds()/(24*3600)
        rval = super(exacttimeind,cls).__new__(cls,ival)
        rval.calendar=calendar
        rval.torig=torig
        return rval

def to_exact_tind(ti,torig=dt.datetime(1900,1,1),calendar='infer'):
    if hasattr(ti,'__len__') and ~isinstance(ti,str): # assume array of datetimes
        return np.array([to_exact_tind(el,torig=torig,calendar=calendar) for el in ti],dtype='object')
    else:
        return exacttimeind(ti,torig=torig,calendar=calendar)
        
def cftnoleap_to_dt(x0):
    if isinstance(x0,cftime.DatetimeNoLeap):
        return dt.datetime(int(x0.year),int(x0.month),int(x0.day),
                           int(x0.hour),int(x0.minute),int(x0.second))
    else: # assume array
        return np.array([cftnoleap_to_dt(el) for el in x0])
        
def lsqfit_md_basic(X,data):
    # assume no NaN values; this is for model results
    # X is nxp covariables; data is nxmxr response variable
    # dimension to do regression on must be 0!
    # calculate linear fit only
    # adapt reshaping code from scipy.signal.detrend
    # put new dimensions at end, except for coefs at front
    data=np.asarray(data)
    dshape = data.shape
    N=dshape[0]
    assert N==len(X) # check correct dimensions
    newdata = np.reshape(data,(N, np.prod(dshape, axis=0) // N)) # // is floor division
    newdata = newdata.copy()  # make sure we have a copy
    b,res,p,svs=np.linalg.lstsq(X,newdata,rcond=None) # res=np.sum((np.dot(X,b)-Y)**2)
    bshp=tuple([2]+list(dshape)[1:])
    b=np.reshape(b,bshp)
    return b

#### fits ####
class basicfit:
    def fromdt(self,tdt):
        if self.calendar=='noleap':
            tdays=to_exact_tind(dt_to_cftnoleap(tdt),torig=self.torig,calendar=self.calendar)
        elif self.calendar=='standard':
            tdays=to_exact_tind(tdt,torig=self.torig,calendar=self.calendar)
        return tdays
    def checktime(self,tdays):
        tchk= tdays[0] if hasattr(tdays,'__len__') else tdays
        if type(tchk)==dt.datetime:
            tdays=self.fromdt(tdays)
            tchk= tdays[0] if hasattr(tdays,'__len__') else tdays
        if not (tchk.calendar==self.calendar and tchk.torig==self.torig):
            raise TypeError('mismatched calendars')
        return tdays
    def deseas(self,yrind,vals):
        yrind[yrind==366]=365 # handle possible leap years
        return np.array([ival-self.seas[iyd-1,...] for iyd, ival in zip(yrind,vals)])
    def targetdetrend(self,tdays,vals,target):
        tdays=self.checktime(tdays)
        target=self.checktime(target)
        rt=len(tdays.shape)
        rc=len(np.shape(self.coef[1,...]))
        return np.asarray(vals)-np.expand_dims(self.coef[1,...],0)*np.expand_dims(tdays-target,tuple(np.arange(rt,rt+rc)))
    def __repr__(self):
        t = type(self)
        attlist=[el for el in self.__dict__.keys()]
        return (f"{t.__module__}.{t.__qualname__}(\n\t coef={self.coef}, "
                    f"fit output={self.out}, time calendar={self.calendar}, "
                    f"\n\t all attributes:{','.join(attlist)})")
    
class linreg:
    # make this a class in order to return object with descriptive attributes
    def __init__(self,X,Y,alpha=0.05):
        A=np.concatenate([np.ones((len(X),1)), np.expand_dims(X,1)],1)
        self.coef = lsqfit_md_basic(A,Y)
    def __repr__(self):
        t = type(self)
        return f"{t.__module__}.{t.__qualname__}(\n\tcoef={self.coef},\n\tSumResSq={self.SumResSq},\n\tp={self.p},\n\tn={self.n},\n\tcov={self.cov},\n\tCI={self.CI}\n\tRsq={self.Rsq})"

class linfit(basicfit):
    def __init__(self,tdays,vals):
        self.calendar=tdays[0].calendar
        self.torig=tdays[0].torig
        fit=linreg(tdays.astype(float),vals)
        self.out=fit
        self.coef=fit.coef

class binseas(basicfit):
    def __init__(self,yind,vals,yrlen):
        self.yrlen=yrlen
        seas=binclim(yind,vals,yrlen)
        self.seas=seas-np.mean(seas,0) # keep the constant in the linear fit, not the seasonality
        
def binclim(yind,vals,yrfac):
    # yind: day or month of year
    # vals: values at each day or month in series; first index must be time
    # yrfac: yind units per year (12 if yind is month of year)
    vals=np.asarray(vals)
    yind=np.asarray(yind)
    sh=list(vals.shape)
    clim=np.empty([yrfac]+sh[1:])
    for ii in range(0,yrfac):
        clim[ii]=np.mean(vals[yind==ii+1,...],0)
    return clim

def binclim2_NM(vals,yrfac): # remove mean so integral of seasonal cycle is zero
    # vals: values at each day or month in series; first index must be time
    # yrfac: yind units per year (12 if yind is month of year)
    vals=np.asarray(vals)
    sh=list(vals.shape)
    clim=np.empty([yrfac]+sh[1:])
    for ii in range(0,yrfac):
        clim[ii]=np.mean(vals[ii::yrfac,...],0)
    clim=clim-np.mean(clim,0)
    return clim

def gsmoothPeriodic(YD,vals,L=30,yearlen=365):
    # YD is day of year (can include fractional component)
    # dim 0 must be time
    allt=np.arange(1,yearlen+1)
    # if vector, add dim:
    if len(np.shape(vals))==1:
        vals=np.expand_dims(vals,axis=1)
    fil=np.empty((len(allt),)+np.shape(vals)[1:])
    s=L/2.355
    for t in allt:
        diff=np.min([np.abs(YD-t),np.abs(YD-t+yearlen), np.abs(YD-t-yearlen)],0)
        weight=_add_dims(np.array([np.exp(-.5*x**2/s**2) if x <=3*L else 0.0 for x in diff]),vals)
        fil[t-1,...]=np.divide(np.nansum(weight*vals,0),np.nansum(weight*~np.isnan(vals),0),
                               out=np.nan*da.array(np.ones(np.shape(vals)[1:])),
                               where=np.nansum(weight*~np.isnan(vals),0)>0)
    return fil # there is no need to remove mean here because it was done in binning if needed; also previously axis was missing

def zarrstash(arr,arrname,temppath,returntype='da'):
    if returntype not in ('da','ds'):
        raise Exception('bad returntype')
    n=len(arr.shape)
    #ch0={**{0:-1},**{i:'auto' for i in range(1,n)}}
    dar=xr.DataArray(arr,name=arrname)
    pathout=os.path.join(temppath,f'{arrname}.zarr')
    mkdirs(pathout)
    dar.to_zarr(pathout,mode='w')
    if returntype=='da':
        return xr.open_zarr(pathout)[arrname].data
    elif returntype=='ds':
        return xr.open_zarr(pathout)[arrname]

### spatial information ############################################################
MPAmaskfile=f'{sourcepath}masks/siteMasksNew.nc'
coralmaskfile=f'{sourcepath}masks/coralMasks.nc'
siteDict=OrderedDict({'NE Canyons': 'Northeast Canyons and Seamounts Marine National Monument',
        'Monterey Bay': 'Monterey Bay National Marine Sanctuary',
        'FL Keys': 'Florida Keys National Marine Sanctuary',
        'La Parg': 'La Parguera Natural Reserve',
        'Isla de Mona': 'Isla de Mona Natural Reserve',
        'Steller':'Steller Sea Lion Rookery Buffer Areas',
        #'Hawaii': 'Papahanaumokuakea Marine National Monument',
        'Humpback':'Hawaiian Islands Humpback Whale National Marine Sanctuary',
        'Pac Remote': 'Pacific Remote Islands Marine National Monument',
        'Samoa': 'National Marine Sanctuary of American Samoa',
        'Marianas':'Marianas Trench Marine National Monument',
        'Coral': 'Grid Cells Containing Coral'})
class maskInfo:
    def __init__(self,sitename,mask):
        self.mask=mask
        self.llats,self.llons=mask.nonzero()
        self.sitename=sitename
        self.maxlat=np.max(glat1x1[mask])
        self.minlat=np.min(glat1x1[mask])
        self.maxlon=np.max(glon1x1[mask])
        self.minlon=np.max(glon1x1[mask])
    def __repr__(self):
        return 'class maskInfo:'+f'{self.__dict__}'

def loadMasks():
    # load MPA sites and coral mask
    sites={}
    for el in siteDict.keys():
        if el=='Coral':
            with xr.open_dataset(coralmaskfile) as fmask:
                locmask=fmask.coralmask.sel(maskType='Boolean').values.astype(bool)
        else:
            with xr.open_dataset(MPAmaskfile) as fmask:
                locmask=fmask.MPAmask.sel(MPA=siteDict[el]).values.astype(bool)
        sites[el]=maskInfo(siteDict[el],locmask)
    return sites
    
### file names #####################################################################
def nc_path(ivar,iscen,yrspan,freq,grid='1x1'):
    bpath=workpath+'calculatedFields/'
    return bpath+f"{iscen}/"\
            +f"calcField.GFDL-ESM4.1.{iscen}.{yrspan[0]}_{yrspan[-1]}."\
            +f"{freq}.{ivar}.{grid}.nc"

def fNameStatsFile(iscen,yrspan,freq,ivar,grid='1x1'):
    bpath=workpath+'calculatedFields/'
    return f"{bpath}{iscen}/"\
            +f"sliceStats.GFDL-ESM4.1.{iscen}.{yrspan[0]}_{yrspan[-1]}."\
            +f"{freq}.{ivar}.{grid}.nc"
    
def exstats_path(ivar,iscen,yrspan,iscenref,yrspanref,freq,exdir,quantile,calcDurAmp=False,grid='1x1'):
    qstring=f"p{quantile*100:.1f}"
    return f"{workpath}exstats/exstats_GFDL-ESM4.1.{iscen}.{yrspan[0]}_{yrspan[-1]}."\
            +f"ref{iscenref}.{yrspanref[0]}_{yrspanref[-1]}.{freq}.{ivar}.{dirstr[exdir]}.{qstring}.{'DurAmp.' if calcDurAmp else ''}{grid}.nc"

def exstats_multi_path(ivars,iscen,yrspan,iscenref,yrspanref,freq,quantile,calcDur=False,grid='1x1'):
    qstring=f"p{quantile*100}_{np.round(1-quantile,2)*100}"
    return (f"{workpath}exstats/exstats_multi_GFDL-ESM4.1.{iscen}.{yrspan[0]}_{yrspan[-1]}."
            f"ref{iscenref}.{yrspanref[0]}_{yrspanref[-1]}.{freq}.{'_'.join([el for el in ivars])}."
            f"{qstring}.{'Dur.' if calcDur else ''}{grid}.nc")

def AMEpath(iscen,yrspan,freq,ivar,exdir,grid='1x1'):
    return f"{workpath}AME/AME.GFDL-ESM4.1.{iscen}.{yrspan[0]}_{yrspan[-1]}."\
            +f"{freq}.{ivar}.{dirstr[exdir]}.{grid}.nc"

def fNameAnnualExtremes(iscen,yrspan,freq,ivar,grid='1x1'):
    return (f"{workpath}annseries/annseries.GFDL-ESM4.1.{iscen}.{yrspan[0]}_{yrspan[-1]}."
            f"{freq}.{ivar}.{grid}.nc")

def MMM_NOAA_path(iscen0,yrspan0,targettime):
    return f"{workpath}AME/MMM_NOAA.{iscen0}.{yrspan0[0]}_{yrspan0[1]}.{targettime:%Y%m%d}.nc"

def DHW_path(iscen,yrspan,iscenMMM,yrspanMMM,dtargetMMM):
    return (f'{workpath}exstats/DHW.{iscen}.{yrspan[0]}_{yrspan[-1]}.MMM{iscenMMM}.'
            f'{yrspanMMM[0]}_{yrspanMMM[-1]}.{dtargetMMM:%Y%m%d}.zarr')

def delt_path(iscen,ivar,refscen,window):
    return (f"{workpath}calculatedFields/deltaSeasAdapt/"
            f"deltaSeasAdapt_{ivar}_{iscen}_ref{refscen}_win{window}.zarr")

def abs_coral_path(iscen,yrspan):
    return f"{workpath}exstats/abscoral.{iscen}.{yrspan[0]}_{yrspan[-1]}.nc"
### setup ##########################################################################
mkdirs(os.path.join(workpath,'calculatedFields/'))

### loading data ###################################################################
sourcelocs={('co3os','monthly','ESM4_historical_D1'):'ESGF/ESM4_historical_D1/co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_*.nc',
            ('co3sataragos','monthly','ESM4_historical_D1'):'ESGF/ESM4_historical_D1/co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_*.nc',
            ('phos','monthly','ESM4_historical_D1'):'ESGF/ESM4_historical_D1/phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_*.nc',
            ('spco2','monthly','ESM4_historical_D1'):'ESGF/ESM4_historical_D1/spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_*.nc',
            ('tos','monthly','ESM4_historical_D1'):'ESGF/ESM4_historical_D1/tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_*.nc',

            ('co3os','daily','ESM4_historical_D1'):'ge_data/ESM4_historical_D1_CRT_1975_2014/ocean_cobalt_daily_sat_z_1x1deg.*.co3_0.nc',
            ('co3sataragos','daily','ESM4_historical_D1'):'ge_data/ESM4_historical_D1_CRT_1975_2014/ocean_cobalt_daily_sat_z_1x1deg.*.co3satarag_0.nc',
            ('phos','daily','ESM4_historical_D1'):'ge_data/ESM4_historical_D1_CRT_1975_2014/ocean_cobalt_daily_sfc_1x1deg.*.phos.nc',
            ('spco2','daily','ESM4_historical_D1'):'ge_data/ESM4_historical_D1_CRT_1975_2014/ocean_cobalt_daily_sfc_1x1deg.*.spco2.nc',
            ('tos','daily','ESM4_historical_D1'):'ESGF/ESM4_historical_D1/tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_*.nc',
            
            ('co3os','monthly','ESM4_ssp126_D1'):'ESGF/ESM4_ssp126_D1/co3os_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_*.nc',
            ('co3sataragos','monthly','ESM4_ssp126_D1'):'ESGF/ESM4_ssp126_D1/co3sataragos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_*.nc',
            ('phos','monthly','ESM4_ssp126_D1'):'ESGF/ESM4_ssp126_D1/phos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_*.nc',
            ('spco2','monthly','ESM4_ssp126_D1'):'ESGF/ESM4_ssp126_D1/spco2_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_*.nc',
            ('tos','monthly','ESM4_ssp126_D1'):'ESGF/ESM4_ssp126_D1/tos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_*.nc',
            
            ('co3os','daily','ESM4_ssp126_D1'):'ge_data/ESM4_ssp126_D1_CRT_2061_2100/ocean_cobalt_daily_sat_z_1x1deg.*.co3_0.nc',
            ('co3sataragos','daily','ESM4_ssp126_D1'):'ge_data/ESM4_ssp126_D1_CRT_2061_2100/ocean_cobalt_daily_sat_z_1x1deg.*.co3satarag_0.nc',
            ('phos','daily','ESM4_ssp126_D1'):'ge_data/ESM4_ssp126_D1_CRT_2061_2100/ocean_cobalt_daily_sfc_1x1deg.*.phos.nc',
            ('spco2','daily','ESM4_ssp126_D1'):'ge_data/ESM4_ssp126_D1_CRT_2061_2100/ocean_cobalt_daily_sfc_1x1deg.*.spco2.nc',
            ('tos','daily','ESM4_ssp126_D1'):'ESGF/ESM4_ssp126_D1/tos_Oday_GFDL-ESM4_ssp126_r1i1p1f1_gr_*.nc',

            ('co3os','monthly','ESM4_ssp370_D1'):'ESGF/ESM4_ssp370_D1/co3os_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_*.nc',
            ('co3sataragos','monthly','ESM4_ssp370_D1'):'ESGF/ESM4_ssp370_D1/co3sataragos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_*.nc',
            ('phos','monthly','ESM4_ssp370_D1'):'ESGF/ESM4_ssp370_D1/phos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_*.nc',
            ('spco2','monthly','ESM4_ssp370_D1'):'ESGF/ESM4_ssp370_D1/spco2_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_*.nc',
            ('tos','monthly','ESM4_ssp370_D1'):'ESGF/ESM4_ssp370_D1/tos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_*.nc',
            
            ('co3os','daily','ESM4_ssp370_D1'):'ge_data/ESM4_ssp370_D1_CRT_2061_2100/ocean_cobalt_daily_sat_z_1x1deg.*.co3_0.nc',
            ('co3sataragos','daily','ESM4_ssp370_D1'):'ge_data/ESM4_ssp370_D1_CRT_2061_2100/ocean_cobalt_daily_sat_z_1x1deg.*.co3satarag_0.nc',
            ('phos','daily','ESM4_ssp370_D1'):'ge_data/ESM4_ssp370_D1_CRT_2061_2100/ocean_cobalt_daily_sfc_1x1deg.*.phos.nc',
            ('spco2','daily','ESM4_ssp370_D1'):'ge_data/ESM4_ssp370_D1_CRT_2061_2100/ocean_cobalt_daily_sfc_1x1deg.*.spco2.nc',
            ('tos','daily','ESM4_ssp370_D1'):'ESGF/ESM4_ssp370_D1/tos_Oday_GFDL-ESM4_ssp370_r1i1p1f1_gr_*.nc',
           }

def _hplus_from_ph(pH):
    return 1e6*10**(-1*pH)

def _hplusPrepFun(ds0):
    name_in=[el for el in list(ds0.keys()) if ('ph' in el) and (not 'average' in el) and (not 'bnds' in el)]
    if len(name_in)==1:
        name_in=name_in[0]
    else:
        raise Exception('Unexpectedly multiple variable containing ph in name:'+str(name_in))
    name_out=name_in.replace('ph','hplus')
    #out=_hplus_from_ph(ds0[name_in]).rename(name_out)
    vattrs=ds0[name_in].attrs
    sattrs={key:val for key, val in vattrs.items() if ('units' in key or 'long_name' in key)}
    sattrs['varname']=name_out
    sattrs['long_name']=f"Hydrogen Ion Concentration"
    sattrs['units']='micromol/l'
    #out=out.assign_attrs(sattrs)
    #ds0[name_out].assign_attrs(sattrs)
    ds0=ds0.assign({name_out:_hplus_from_ph(ds0[name_in]).assign_attrs(sattrs)})
    ds0=ds0.drop_vars(name_in) # drop->drop_vars
    return ds0
    
def _genericLoad1varPfun(ivar,freq,iscen,yrspan,chunks,pfun,name_in):
    ds=xrload(name_in,freq,iscen,yrspan,mfds_kw={'chunks':chunks,'parallel':True,
                       'preprocess':pfun})
    datar=ds[ivar].chunk(dict(time=-1)) # fix time chunk so not split
    sattrs=datar.attrs # already processed by prepfun
    dims={el:ds[el].copy() for el in ds.sizes.keys()}
    return datar.data,ds.time.values, dims, sattrs, ds

def genflist(ivar,freq,iscen,yrspan):
    fl0=sorted(glob.glob(sourcepath+sourcelocs[(ivar,freq,iscen)]))
    def _containsyrs(fstr,yrs):
        ystr=re.search('[0-9]+-[0-9]{4}',fstr).group()
        ystart=int(ystr[:4])
        yend=int(ystr[-4:])
        return (yend>=yrs[0])&(ystart<=yrs[1])
    return [el for el in fl0 if _containsyrs(el,yrspan)]

def xrload(ivar, freq, iscen, yrspan, mfds_kw={}):
    # note: returned set is inclusive of end year yrspan[1] as well as start year yrspan[0]
    #       can provide preprocess=prepfun(ds) in mfds_kw to preprocess individual files
    files=genflist(ivar, freq, iscen, yrspan)
    print(dt.datetime.now())
    ds=xr.open_mfdataset(files,**mfds_kw).sel(time=slice(cftime.datetime(yrspan[0],1,1,calendar='noleap'),
                                                              cftime.datetime(yrspan[-1]+1,1,1,calendar='noleap')))
    print(dt.datetime.now())
    return ds

def xrloadMulti(ivars, freq, iscen, yrspan, mfds_kw={}):
    files={}
    for ivar in ivars:
        files[ivar]=genflist(ivar, freq, iscen, yrspan)
    flist=[a for b in list(files.values()) for a in b]
    ds=xr.open_mfdataset(flist,**mfds_kw).sel(time=slice(cftime.datetime(yrspan[0],1,1,calendar='noleap'),
                                                              cftime.datetime(yrspan[-1]+1,1,1,calendar='noleap')))
    return ds

varLoadFunDict={'hplusos':functools.partial(_genericLoad1varPfun,
                                                   pfun=_hplusPrepFun,
                                                   name_in='phos'),
               }
    
def loadSlab(ivar,freq,iscen,yrspan,chnkdef={}):
    if os.path.exists(nc_path(ivar,iscen,yrspan,freq)):
        ds=xr.open_dataset(nc_path(ivar,iscen,yrspan,freq),chunks=chnkdef)
        datar0=ds[ivar].chunk(chnkdef)
        mod_tnl=ds.time.values
        dims={el:ds[el].copy() for el in ds.sizes.keys()}
        sattrs=datar0.attrs
        datar=datar0.data
    elif ivar in varLoadFunDict.keys():
        datar,mod_tnl,dims, sattrs,ds=varLoadFunDict[ivar](ivar,freq,iscen,yrspan,chnkdef)
    else:
        ds=xrload(ivar,freq,iscen,yrspan,mfds_kw={'parallel':True,'chunks':chnkdef})#'chunks':chunks,
        datar0=ds[ivar].chunk(chnkdef)
        mod_tnl=ds.time.values
        dims={el:ds[el].copy() for el in ds.sizes.keys()}
        vattrs=datar0.attrs
        sattrs={key:val for key, val in vattrs.items() if ('units' in key or 'name' in key)}
        sattrs['varname']=ivar
        datar=datar0.data
    for el in [el for el in list(ds.variables) if not el in ('average_DT','average_T1','average_T2','time','time_bnds') \
               and not el.startswith('lat') and not el.startswith('lon')]:
        if 'units' in ds[el].attrs:
            print(el,ds[el].units)
    # check variable length is correct
    if freq=='daily':
        yl=365
    elif freq=='monthly':
        yl=12
    else:
        raise Exception(f'unknown freq: {freq}')
    ll=(yrspan[-1]-yrspan[0]+1)*yl
    if not ((len(ds.time.values)==ll) and (datar.shape[0]==ll)):
        print('times loaded:')
        for el in ds.time.values:
            print(el)
        raise Exception('variable has incorrect length along time dimension:{len(ds.time.values)},{datar.shape[0]},{ll}')
    return datar, mod_tnl, dims, sattrs, ds

def save_omega_arag_0(iscen,yrspan,freq,recalc=False):
    path_final=nc_path('omega_arag_0',iscen,yrspan,freq)
    if recalc or not os.path.exists(path_final):
        ivars=['co3os','co3sataragos']
        ds = xrloadMulti(ivars,freq,iscen,yrspan)
        sattrs={'long_name':'Aragonite Saturation State','units':''}
        try:
            vv=ds['co3os']/ds['co3sataragos']
        except KeyError:
            vv=ds['co3']/ds['co3satarag']
        vv=vv.rename('omega_arag_0')
        if len(vv.shape)==4:
            vv=vv.isel(z_l=0)
        vv=vv.chunk(dict(time=-1,lat=chunksdef['lat'],lon=chunksdef['lon']))
        vv.attrs.update(sattrs)
        ds1=vv.to_dataset()
        mkdirs(path_final)
        ds1.to_netcdf(path_final)
    return path_final

def openAME(iscen,ivar,exdir):
    if iscen.startswith('ESM4_historical_D'):
        amep=AMEpath(iscen,yrspanFull['ESM4_historical_D1'],'monthly',ivar,exdir,'1x1')
        AME=xr.open_dataset(glob.glob(amep)[0],chunks={'year':-1})
        ll=(yrspanFull['ESM4_historical_D1'][-1]-yrspanFull['ESM4_historical_D1'][0]+1)
    elif iscen.endswith('_D1'):
        amep0=AMEpath('ESM4_historical_D1',yrspanFull['ESM4_historical_D1'],'monthly',ivar,exdir,'1x1')
        amep=AMEpath(iscen,[2015,2100],'monthly',ivar,exdir,'1x1')
        AME=xr.open_mfdataset([glob.glob(amep0)[0],glob.glob(amep)[0]],chunks={'year':-1})
        ll=(2100-yrspanFull['ESM4_historical_D1'][0]+1)
    else:
        raise Exception('Not implemented')
    if not (len(AME.year.values)==ll): # check length along time dimension
        print('years loaded:')
        for el in ds.year.values:
            print(el)
        raise Exception('variable has incorrect length along time dimension:{len(ds.year.values)},{ll}')
    return AME

def loadMonthlys(iivar,iiscen,ichdef0):
    print(iivar,iiscen)
    if iiscen.startswith('ESM4_historical_D'):
        __,__,__,__,dsm = loadSlab(iivar,'monthly',iiscen,yrspanFull['ESM4_historical_D1'],ichdef0)
        print(np.shape(dsm[iivar]))
        dsm2=dsm.coarsen(time=12).construct(time=("year","month"))
        dsm2['year']=np.arange(yrspanFull['ESM4_historical_D1'][0],2015)
        dsm2['month']=np.arange(1,13)
    elif iiscen.endswith('_D1'):
        __,__,__,__,dsm0 = loadSlab(iivar,'monthly','ESM4_historical_D1',yrspanFull['ESM4_historical_D1'],ichdef0)
        __,__,__,__,dsm1 = loadSlab(iivar,'monthly',iiscen,yrspanFull[iiscen],ichdef0)
        dsm=xr.concat([dsm0,dsm1],dim='time')
        print(np.shape(dsm[iivar]))
        dsm2=dsm.coarsen(time=12).construct(time=("year","month"))
        dsm2['year']=np.arange(yrspanFull['ESM4_historical_D1'][0],yrspanFull[iiscen][-1]+1)
        dsm2['month']=np.arange(1,13)
    else:
        raise Exception('Not implemented')
    return dsm2.reset_coords()

def load_stats(scens=None,varnames=None,yrspans=None,freq='daily'):
    # inputs should be lists
    # if provided, yrspans must correspond to scens listed, in order
    # yrspans default to whole 40-yr segment if not supplied
    if scens is None:
        scens=['ESM4_historical_D1','ESM4_ssp126_D1','ESM4_ssp370_D1',]
    if varnames is None:
        varnames=['tos','omega_arag_0','hplusos','spco2']
    if yrspans is None:
        yrspans=[dictScenDates[iscen] for iscen in scens]
    ff={}
    for iscen,yrspan in zip(scens,yrspans):
        ff[iscen]={}
        for ivar in varnames:
            fname=fNameStatsFile(iscen,yrspan,freq,ivar,grid='1x1')
            try:
                ff[iscen][ivar]=xr.open_dataset(fname)
            except:
                print(f'Could not load ({iscen},{ivar},{yrspan},{freq}):\n{fname}')
                ff[iscen][ivar]=None
    return ff

##### Calculations #############################################################    
def run_calcs(ivar,freq,iscen,yrspan,recalc=False,ydqtcalc=False):
    stmp=os.path.join(tmpdir,f"{ivar}_{iscen}_{freq}")
    if os.path.exists(stmp):
        shutil.rmtree(stmp)
    mkdirs(os.path.join(stmp,'null'))
    yl={'daily':365,'monthly':12}
    ylen=yl[freq]
    q=quantiledef
    fileout=fNameStatsFile(iscen,yrspan,freq,ivar,grid='1x1')
    if not recalc:
        if os.path.exists(fileout):
            return # file already exists; go to next and don't recalculate
    if recalc and os.path.exists(fileout):
        os.remove(fileout)
    mkdirs(fileout)
    chnkdef=chunksdef #{'time':-1,'lat':90,'lon':90}
    cluster = LocalCluster(n_workers=5,threads_per_worker=1,)
    client = Client(cluster)
    datar,mod_tnl,dims, sattrs,ds=loadSlab(ivar,freq,iscen,yrspan,chnkdef)
    chnk=dict(zip(['time','lat','lon'],datar.chunksize))
    # tc is midpoint of slice
    tc0=.5*(yrspan[0]+yrspan[-1]+1)
    tc=cftime.DatetimeNoLeap(int(tc0),1,1,0)+dt.timedelta(days=365*(tc0-int(tc0)))
    mod_tind=to_exact_tind(mod_tnl,torig=tc)#mod_tnl[0]) # changed from start of slice to midpoint
    mod_yd=ydNLfromcftNL(mod_tnl)
    mod_val=zarrstash(datar,'mod_val',stmp)#.data # mod val is dask array
    print('chnksize:',mod_val.chunksize)
    
    mean_0=mod_val.mean(axis=0,keepdims=True).compute()
    var_0=mod_val.var(axis=0,keepdims=True).compute()
    qt_0=da.map_blocks(np.quantile,mod_val,q,axis=0,
                    dtype=np.float64,drop_axis=[0],new_axis=0,
                       chunks=(len(q),chnk['lat'],chnk['lon'])).compute()
    
    ti=mod_tind.astype(float)
    A=np.concatenate([np.ones((len(ti),1)), np.expand_dims(ti,1)],1)
    coef0=da.map_blocks(lsqfit_md_basic,A,mod_val,
                        dtype=np.float64,drop_axis=[0],new_axis=0,
                        chunks=(2,chnk['lat'],chnk['lon']))
    
    yest0=ti.reshape((len(ti),1,1))*coef0[1,:,:].reshape((1,)+np.shape(coef0[1,:,:]))+\
                        coef0[0,:,:].reshape((1,)+np.shape(coef0[0,:,:]))
    vals0=zarrstash((mod_val-yest0).rechunk({-1,chnk['lat'],chnk['lon']}),'vals0',stmp)

    fitseasR=da.map_blocks(binclim2_NM,vals0,ylen,
                        dtype=np.float64,drop_axis=[0],new_axis=0,
                        chunks=(ylen,chnk['lat'],chnk['lon'])).compute()
    gs=gsmoothPeriodic(np.arange(1,366),fitseasR,L=30)
    fitseas=da.from_array(gs,chunks=[365,chnk['lat'],chnk['lon']])
    # remove seasonal cycle (with zero annual mean)
    rt=int(len(mod_val)/len(fitseas))
    val_ds=zarrstash(mod_val-da.tile(fitseas,[rt,1,1]).rechunk(chunks=mod_val.chunksize),'val_ds',stmp)
    # calculate linear fit after mean seasonality removed
    coef=da.map_blocks(lsqfit_md_basic,A,val_ds,
                       dtype=np.float64,drop_axis=[0],new_axis=0,chunks=(2,chnk['lat'],chnk['lon'])).persist()
    # detrend relative to midpoint of slice (ti=0) [leave on constant term]
    yest_lin=ti.reshape((len(ti),1,1))*coef[1,:,:].reshape((1,)+np.shape(coef[1,:,:])).persist() #+\
                            #coef[0,:,:].reshape((1,)+np.shape(coef[0,:,:]))
    val_dt=zarrstash(mod_val-yest_lin,'val_dt',stmp)
    val_dtds=zarrstash(val_ds-yest_lin,'val_dtds',stmp)
    del yest_lin
    qt_dt=da.map_blocks(np.quantile,val_dt,q,axis=0,
                    dtype=np.float64,drop_axis=[0,],new_axis=0,chunks=(len(q),chnk['lat'],chnk['lon']))
    mean_dt=val_dt.mean(axis=0,keepdims=True)
    var_dt=val_dt.var(axis=0,keepdims=True)
    
    qt_ds=da.map_blocks(np.quantile,val_ds,q,axis=0,
                    dtype=np.float64,drop_axis=[0],new_axis=0,chunks=(len(q),chnk['lat'],chnk['lon']))
    qt_dtds=da.map_blocks(np.quantile,val_dtds,q,axis=0,
                    dtype=np.float64,drop_axis=[0],new_axis=0,chunks=(len(q),chnk['lat'],chnk['lon']))
    mean_dtds=val_dtds.mean(axis=0,keepdims=True)
    var_dtds=val_dtds.var(axis=0,keepdims=True)
    var_seas=np.var(fitseas,axis=0,keepdims=True)

    #mean_0   = mean_0.compute()  
    #var_0    = var_0.compute()    
    #qt_0     = qt_0.compute() 
    coef     = coef.compute() 
    #fitseasR = fitseasR.compute() 
    #fitseas  = fitseas.compute() 
    mean_dt  = mean_dt.compute() 
    var_dt   = var_dt.compute() 
    qt_dt    = qt_dt.compute() 
    qt_ds    = qt_ds.compute() 
    mean_dtds= mean_dtds.compute() 
    var_dtds = var_dtds.compute() 
    var_seas = var_seas.compute() 
    qt_dtds  = qt_dtds.compute() 
    dsout=xr.Dataset(data_vars=dict(mean=(['time','lat','lon'],mean_0),
                                 var=(['time','lat','lon'],var_0),
                                 qt=(('quantile','lat','lon'),qt_0),
                                 linfitcoef=(('b','lat','lon'),coef),
                                 binseas=(('YD','lat','lon'),fitseasR),
                                 seas=(('YD','lat','lon'),fitseas),
                                 mean_dt=(('time','lat','lon'),mean_dt),
                                 var_dt=(('time','lat','lon'),var_dt),
                                 qt_dt=(('quantile','lat','lon'),qt_dt),
                                 qt_ds=(('quantile','lat','lon'),qt_ds),
                                 mean_dtds=(('time','lat','lon'),mean_dtds),
                                 var_dtds=(('time','lat','lon'),var_dtds),
                                 var_seas=(('time','lat','lon'),var_seas),
                                 qt_dtds=(('quantile','lat','lon'),qt_dtds)),
                attrs=sattrs,
                coords={'time':[tc,],'lat':dims['lat'],'lon':dims['lon'],
                        'YD':np.arange(1,ylen+1),'quantile':q,'b':[0,1]})
    dsout.to_netcdf(fileout,mode='a')
   
    if ydqtcalc:#quantiles by yearday
        # check chunks
        def _inwindow(yd,ydtarget,halfwindow):
            if (yd>=ydtarget-halfwindow) & (yd<=ydtarget+halfwindow)|\
                (yd-365>=ydtarget-halfwindow) & (yd-365<=ydtarget+halfwindow)|\
                (yd+365>=ydtarget-halfwindow) & (yd+365<=ydtarget+halfwindow):
                return True
            else:
                return False
        sattrs['year-day quantile Descript']='full series fits are applied to dt and ds variables for ydqt variables'

        halfwindow=2
        sattrs['ydqt halfwindow']=f"{halfwindow}"
        qtlist=[]
        qt_dtlist=[]
        qt_dslist=[]
        qt_dtdslist=[]
        #fig,ax=plt.subplots(1,1)
        ydlist=np.arange(0,365,5)
        for it in ydlist:
            print(it)
            tsel=np.array([_inwindow(iit%365,it,halfwindow) for iit in range(0,len(ds.time))])
            #ax.plot(np.arange(0,365),np.where(tsel[:365],it,np.nan),'k*')
            ymod=mod_val[tsel,...]#ds['mod_val'].isel(time=tsel).values
            ymod_dt=val_dt[tsel,...]
            ymod_ds=val_ds[tsel,...]
            ymod_dtds=val_dtds[tsel,...]
            yqt=da.map_blocks(np.quantile,ymod,q,axis=0,
                                dtype=np.float64,drop_axis=[0],new_axis=0,
                                   chunks=(len(q),chnk['lat'],chnk['lon']))
            yqt_dt=da.map_blocks(np.quantile,ymod_dt,q,axis=0,
                                dtype=np.float64,drop_axis=[0],new_axis=0,
                                   chunks=(len(q),chnk['lat'],chnk['lon']))
            yqt_ds=da.map_blocks(np.quantile,ymod_ds,q,axis=0,
                                dtype=np.float64,drop_axis=[0],new_axis=0,
                                   chunks=(len(q),chnk['lat'],chnk['lon']))
            yqt_dtds=da.map_blocks(np.quantile,ymod_dtds,q,axis=0,
                                dtype=np.float64,drop_axis=[0],new_axis=0,
                                   chunks=(len(q),chnk['lat'],chnk['lon']))
            #print('A')
            qtlist.append(yqt.compute())
            #print('B')
            qt_dtlist.append(yqt_dt.compute())
            #print('C')
            qt_dslist.append(yqt_ds.compute())
            #print('D')
            qt_dtdslist.append(yqt_dtds.compute())
        print('done loop')
        ydqt2=np.stack(qtlist,axis=0)#fitseas=gsmoothPeriodic(np.arange(1,366),fitseasR,L=30)
        ydqt_dt2=np.stack(qt_dtlist,axis=0)
        ydqt_ds2=np.stack(qt_dslist,axis=0)
        ydqt_dtds2=np.stack(qt_dtdslist,axis=0)

        client.close()
        cluster.close()
        ydqt2S=np.empty((365,len(q),180,360))
        ydqt_dt2S=np.empty((365,len(q),180,360))
        ydqt_ds2S=np.empty((365,len(q),180,360))
        ydqt_dtds2S=np.empty((365,len(q),180,360))
        for ix in range(0,len(q)):
            print(dt.datetime.now())
            ydqt2S[:,ix,:,:]=gsmoothPeriodic(ydlist+1,ydqt2[:,ix,:,:],L=30)
            ydqt_dt2S[:,ix,:,:]=gsmoothPeriodic(ydlist+1,ydqt_dt2[:,ix,:,:],L=30)
            ydqt_ds2S[:,ix,:,:]=gsmoothPeriodic(ydlist+1,ydqt_ds2[:,ix,:,:],L=30)
            ydqt_dtds2S[:,ix,:,:]=gsmoothPeriodic(ydlist+1,ydqt_dtds2[:,ix,:,:],L=30)

        # ydqt2S=gsmoothPeriodic(ydlist+1,ydqt2,L=30)#fitseas=gsmoothPeriodic(np.arange(1,366),fitseasR,L=30)
        # ydqt_dt2S=gsmoothPeriodic(ydlist+1,ydqt_dt2,L=30)
        # ydqt_ds2S=gsmoothPeriodic(ydlist+1,ydqt_ds2,L=30)
        # ydqt_dtds2S=gsmoothPeriodic(ydlist+1,ydqt_dtds2,L=30)
        
        dsouti=xr.Dataset(data_vars={'ydqt2':(('YD5','quantile','lat','lon'),ydqt2),
                                     'ydqt_dt2':(('YD5','quantile','lat','lon'),ydqt_dt2),
                                     'ydqt_ds2':(('YD5','quantile','lat','lon'),ydqt_ds2),
                                     'ydqt_dtds2':(('YD5','quantile','lat','lon'),ydqt_dtds2),
                                    'ydqt2S':(('YD','quantile','lat','lon'),ydqt2S),
                                     'ydqt_dt2S':(('YD','quantile','lat','lon'),ydqt_dt2S),
                                     'ydqt_ds2S':(('YD','quantile','lat','lon'),ydqt_ds2S),
                                     'ydqt_dtds2S':(('YD','quantile','lat','lon'),ydqt_dtds2S),},
                    attrs=sattrs,
                    coords={'time':[tc,],'lat':dims['lat'],'lon':dims['lon'],
                            'YD':np.arange(1,ylen+1),'YD5':ydlist+1,'quantile':q,'b':[0,1]})
        print('Before save',dt.datetime.now())
        sys.stdout.flush()
        dsouti.to_netcdf(fileout,mode='a')
        print('After save',dt.datetime.now())
        sys.stdout.flush()

    if type(ds)==list:
        for el in ds:
            el.close()
    else:
        ds.close()
    shutil.rmtree(stmp)
    return

def calc_AME(ivar,iscen,edir=1,chdef0={'time':5,'lat':-1,'lon':-1}):
    freq='monthly'
    chdefout={'year':-1,'lat':-1,'lon':-1,}
    if iscen in yrspanFull.keys():
        yrspan=yrspanFull[iscen]
        res='1x1'
        pout=AMEpath(iscen,yrspan,freq,ivar,edir,res)
        if os.path.exists(pout):
            print('path exists, not recalculating:',pout)
            return pout
        else:
            dat,tt,dims,sattrs,ds = loadSlab(ivar,freq,iscen,yrspan,chdef0)
    else:
        raise Exception('error')
    if edir==1:
        fstr='Maximum'
        ame=ds[ivar].groupby("time.year").max()
        ame_out=ame.chunk(chunks=chdefout)
    elif edir==-1:
        fstr='Minimum'
        ame=ds[ivar].groupby("time.year").min()
        ame_out=ame.chunk(chunks=chdefout)
    try:
        ame_out.attrs['long_name']=f'Annual Monthly {fstr} '+ame_out.attrs['long_name']
    except:
        ame_out.attrs['long_name']=f'Annual Monthly {fstr} {ivar}'
    mkdirs(pout)
    ame_out.to_netcdf(pout,mode='w') 
    ds.close()
    return pout

def rollingAMEclim(ivar,exdir,iscen,window,yrspantarget):
    # load appropriate time series of AMEs and calculate rolling climatology
    # currently will not work for pi-control
    AME=openAME(iscen,ivar,exdir)
    rclim=AME.rolling(dim={'year':window},center=False).mean().\
            shift({'year':1}).dropna("year",how="all")
    out=rclim.sel(year=slice(yrspantarget[0],yrspantarget[1])).load()
    AME.close()
    return out

def run_annual_extremes(ivar,freq,iscen,yrspan,recalc=False):
    stmp=os.path.join(tmpdir,f"{ivar}_{iscen}_{freq}")
    if os.path.exists(stmp):
        shutil.rmtree(stmp)
    yl={'daily':365,'monthly':12}
    if exdirs[ivar]>0:
        q=np.array([0.90,0.91,0.92,0.93,0.94,0.95,0.96,0.97,0.98,0.99,1])
    else:
        q=np.array([0,0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.10,])
    filestats=fNameStatsFile(iscen,yrspan,freq,ivar,grid='1x1')
    fileout=fNameAnnualExtremes(iscen,yrspan,freq,ivar,grid='1x1')
    if os.path.exists(fileout):
        if recalc:
            os.remove(fileout)
        else:
            print('file already exists:',fileout)
            return # file already exists; go to next and don't recalculate
    mkdirs(fileout)
    datar,mod_tnl,dims, sattrs,ds=loadSlab(ivar,freq,iscen,yrspan,chunksdef)
    # tc is midpoint of slice
    tc0=.5*(yrspan[0]+yrspan[-1]+1)
    tc=cftime.DatetimeNoLeap(int(tc0),1,1,0)+dt.timedelta(days=365*(tc0-int(tc0)))
    mod_tind=to_exact_tind(mod_tnl,torig=tc) # changed from start of slice to midpoint
    ti=mod_tind.astype(float)
    mod_val=ds[ivar]#.data # use dataset version to access quantile method
    an_mean_0=mod_val.groupby(ds.time.dt.year).mean()
    an_qt_0=mod_val.groupby(ds.time.dt.year).quantile(q)
    
    dsout=xr.Dataset(data_vars=dict(an_mean=(('year','lat','lon'),an_mean_0.data),
                                 an_qt=(('year','lat','lon','quantile'),an_qt_0.data)),
                attrs=sattrs,
                coords={'year':an_qt_0.year,'lat':ds.lat,'lon':ds.lon,
                        'quantile':q})
    print('start save', dt.datetime.now())
    dsout.to_netcdf(fileout,mode='a') 
    print('saved: ',ivar, dt.datetime.now())
    sys.stdout.flush()
    
    if type(ds)==list:
        for el in ds:
            el.close()
    else:
        ds.close()
    return fileout
###################################################################################################
def eventcalcs(diffvec):
    inds=np.arange(0,len(diffvec))
    starts=inds[diffvec==1]
    if len(starts)>0:
        ends=inds[diffvec==-1]
        durs=ends-starts
        count_gt0=len(starts)
        count_gt5=(durs>5).sum()
        count_gt30=(durs>30).sum()
        count_gt180=(durs>180).sum()
        mdur_gt0=np.mean(durs)
        mdur_gt5=np.mean(durs[durs>5])
        maxdur=np.max(durs)
        return np.array([count_gt0,count_gt5,count_gt30,count_gt180,mdur_gt0,mdur_gt5,maxdur])
    else:
        return np.array([0.,0.,0.,0.,0.,0.,0.])
##############################################################################
class scenData:
    def __init__(self,iivar,ifreq,iiscen,ichdef0,exdir=None,calcDurAmp=False):
        self.ivar=iivar
        self.freq=ifreq
        self.iscen=iiscen
        self.yrspan=dictScenDates[iiscen]
        self.chdef0=ichdef0
        self.dat,self.tt,self.dims,self.sattrs,self.ds = loadSlab(iivar,ifreq,iiscen,self.yrspan,ichdef0)
        self.tdt=cftnoleap_to_dt(self.tt)
        self.exdir=exdirs[iivar] if exdir is None else exdir
        ck=self.dat.chunksize
        self.dat=self.dat.rechunk(chunks=(-1,ck[1],ck[2]))
        self.chksz=self.dat.chunksize
        self.calcDurAmp=calcDurAmp
        stmp=os.path.join(tmpdir,f"{iivar}_{iiscen}_{ifreq}/")
        if os.path.exists(stmp):
            shutil.rmtree(stmp)
        mkdirs(os.path.join(stmp,'null'))
        self.stmp=stmp
    def regionsel(self,maskInfo,siteKey):
        self.maskInfo=maskInfo
        self.reg=siteKey
        self.ds=self.ds.isel(lat=xr.DataArray(maskInfo.llats,dims='cellnum'),
                                lon=xr.DataArray(maskInfo.llons,dims='cellnum')).load()
        self.dat=self.dat.reshape((np.shape(self.dat)[0],-1),limit='500MiB')\
                        [:,np.ravel_multi_index([maskInfo.llats,maskInfo.llons],
                                                np.shape(self.dat)[1:])].persist()
        self.chksz=self.dat.chunksize # update
        print('new shape:',np.shape(self.dat))
    def setrefstats(self,hstats,refscen,refyrspan):
        self.hstats=hstats
        self.refscen=refscen
        self.refspan=refyrspan
        if hasattr(self,'jj'):
            self.hstatsij=hstats.isel(lat=jj,lon=ii)
        if hasattr(self,'reg'):
            self.hstats=hstats.isel(lat=xr.DataArray(self.maskInfo.llats,dims='cellnum'),
                                lon=xr.DataArray(self.maskInfo.llons,dims='cellnum'))
    def setquant(self,quant):
        self.q=quant if self.exdir==1 else np.round(1-quant,2)
    def setAME(self,fAME):
        self.fAME=fAME.chunk({"year":-1})
        if hasattr(self,'jj'):
            self.fAMEij=self.fAME.isel(lat=jj,lon=ii)
        if hasattr(self,'reg'):
            self.fAME=self.fAME.isel(lat=xr.DataArray(self.maskInfo.llats,dims='cellnum'),
                                lon=xr.DataArray(self.maskInfo.llons,dims='cellnum'))
    def rollAME(self,window):
        rclim=self.fAME.rolling(dim={'year':window},center=False).mean().\
            shift({'year':1}).dropna("year",how="all").sel(year=slice(self.yrspan[0],self.yrspan[1])).load()
        return rclim
    def rollSeas(self,window):
        # how to apply adaptation to seasonal calculations:
        # start from 1975-2014 threshold and to each day add an interpolated delta equal to the difference betwen the adaptation period mean monthly mean and the reference period mean monthly mean
        # try to load; if not, calculate and save
        deltpath=delt_path(self.iscen,self.ivar,self.refscen,window)
        if os.path.exists(deltpath):
            dz=xr.open_zarr(deltpath,chunks=self.chdef0)
            delt3=dz['delta'].data
        else:
            print('recalculating delta: no', deltpath)
            tmpfpath=os.path.join(tmpdir,f"tmpshift_{self.ivar}_{self.iscen}_{self.refscen}_{window}_{dt.datetime.now().strftime('%Y%m%d%H%M%s')}.zarr")
            if os.path.exists(tmpfpath):
                shutil.rmtree(tmpfpath)
            dsm=loadMonthlys(self.ivar,self.iscen,self.chdef0)
            dsmref0=dsm if self.iscen==self.refscen else loadMonthlys(self.ivar,self.refscen,self.chdef0)
            dsmref=dsmref0[self.ivar].sel(year=slice(self.refspan[0],self.refspan[-1])).mean(dim='year')
            dsmroll=dsm[self.ivar].rolling(dim={'year':window},center=False).mean().\
                        shift({'year':1}).dropna("year",how="all").\
                        sel(year=slice(self.yrspan[0],self.yrspan[1])).load()
            shift0=dsmroll-dsmref
            yds=[ydfromdt(dt.datetime(2023,ii,15)) for ii in range(1,13)]
            sh=zarrstash(shift0.transpose("month","lat","lon","year"),'sh',tmpfpath)
            delt=np.empty((365,180,360,self.yrspan[-1]-self.yrspan[0]+1))
            for ii in range(0,self.yrspan[-1]-self.yrspan[0]+1):
                print(ii,dt.datetime.now())
                delt[:,:,:,ii]=gsmoothPeriodic(yds,sh[:,:,:,ii],L=30)
            delt2=delt.transpose([3,0,1,2])
            delt3=delt2.reshape((-1,)+np.shape(delt2)[2:])
            #print(deltpath)
            mkdirs(deltpath)
            dsout=xr.Dataset({'delta':(('time','lat','lon'),delt3)},
                            coords={'time': self.ds.time,'lat': self.ds.lat,'lon': self.ds.lon})
            dsout.to_zarr(deltpath)
            shutil.rmtree(tmpfpath)
        if hasattr(self,'reg'):
            delt3=delt3.reshape((np.shape(delt3)[0],-1),limit='500MiB')\
                        [:,np.ravel_multi_index([self.maskInfo.llats,self.maskInfo.llons],
                                                np.shape(delt3)[1:])].persist()
        return delt3
    def rollSeas60(self):
        self.delta_rollSeas60=self.rollSeas(60)
    def rollSeas100(self):
        self.delta_rollSeas100=self.rollSeas(100)
    def rollSeas_N(self,N):
        setattr(self,f"delta_rollSeas_{N}",self.rollSeas(N))
    def amps(self,thresh3d): #time,lat,lon
        if type(thresh3d)==np.ndarray:
            amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
                                     da.from_array(thresh3d,chunks=self.chksz)),
                           axis=0).compute()
        else:
            amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
                                     da.rechunk(thresh3d,chunks=self.chksz)),
                           axis=0).compute()
        return amp
    def exA_all(self,calcDurAmpA=False):
        thresh=self.hstats['qt'].sel(quantile=self.q).data #values
        self.threshA=thresh
        #print('A',type(thresh)) # np.ndarray
        # extremes relative to qt95, ignoring seasonality
        if self.exdir==1:
            self.is_exA=self.dat>thresh
            self.amp_exA=da.where(self.is_exA,self.dat-thresh,np.nan)
        elif self.exdir==-1:
            self.is_exA=self.dat<thresh
        self.is_exA_sum=self.is_exA.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpA:
            self.exA_dur=da.apply_along_axis(meanlen,0,self.is_exA).compute()
            self.exA_amp=self.amps(da.expand_dims(thresh,axis=0))
        #self.exA_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.from_array(da.expand_dims(thresh,axis=0),chunks=(-1,self.chksz[1],self.chksz[2]))),
        #                       axis=0).compute()
    def exC_all(self,calcDurAmpC=False):
        rt=self.yrspan[-1]-self.yrspan[0]+1
        print('rt:',rt)
        self.threshC=gsmoothPeriodic(np.arange(0,365,5)+1,
                                     self.hstats['ydqt2'].sel(quantile=self.q).values,L=10)
        #print(np.shape(self.threshC))
        thresh=da.rechunk(da.tile(self.threshC,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)
        #print('C',type(thresh)) # np.ndarray
        if self.exdir==1:
            self.is_exC=self.dat>thresh
        elif self.exdir==-1:
            self.is_exC=self.dat<thresh
        self.is_exC_sum=self.is_exC.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpC:
            self.exC_dur=da.apply_along_axis(meanlen,0,self.is_exC).compute()
            self.exC_amp=self.amps(thresh)
        #self.exC_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.from_array(thresh,chunks=self.chksz)),
        #                       axis=0).compute()
    def exD_all(self,calcDurAmpD=False):
        rt=self.yrspan[-1]-self.yrspan[0]+1
        #print(rt)
        self.threshD=self.hstats['seas'].values+self.hstats['qt_ds'].sel(quantile=self.q).values 
        #print(np.shape(self.threshD))
        thresh=da.rechunk(da.tile(self.threshD,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)
        #print('D',type(thresh))# da.core.Array
        if self.exdir==1:
            self.is_exD=self.dat>thresh
        elif self.exdir==-1:
            self.is_exD=self.dat<thresh
        self.is_exD_sum=self.is_exD.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpD:
            self.exD_dur=da.apply_along_axis(meanlen,0,self.is_exD).compute()
            self.exD_amp=self.amps(thresh)
        #self.exD_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.rechunk(thresh,chunks=self.chksz)),#self.chksz[1],self.chksz[2]))),
        #                       axis=0).compute()
    def exE60_all(self,calcDurAmpE60=False):# 60 year
        rclim=self.rollAME(60)
        self.threshE60=rclim[self.ivar].data
        thresh=self.threshE60.repeat(365,axis=0) # this is correct use of repeat: individual array elements are repeated 365 times (filling out the year from an annual value)
        #print('E60',type(thresh)) # np.ndarray
        if self.exdir==1:
            self.is_exE60=self.dat>thresh
        elif self.exdir==-1:
            self.is_exE60=self.dat<thresh
        self.is_exE60_sum=self.is_exE60.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpE60:
            self.exE60_dur=da.apply_along_axis(meanlen,0,self.is_exE60).compute()
            self.exE60_amp=self.amps(thresh)
        #self.exE60_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.from_array(thresh,chunks=self.chksz)),
        #                       axis=0).compute()
    def exE100_all(self,calcDurAmpE100=False):# 100 year
        rclim=self.rollAME(100)
        self.threshE100=rclim[self.ivar].data
        thresh=self.threshE100.repeat(365,axis=0) # this is correct use of repeat: individual array elements are repeated 365 times (filling out the year from an annual value)

        #print('E100',type(thresh)) # np.ndarray
        if self.exdir==1:
            self.is_exE100=self.dat>thresh
        elif self.exdir==-1:
            self.is_exE100=self.dat<thresh
        self.is_exE100_sum=self.is_exE100.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpE100:
            self.exE100_dur=da.apply_along_axis(meanlen,0,self.is_exE100).compute()
            self.exE100_amp=self.amps(thresh)
    def exE_N_all(self,N):# N 
        rclim=self.rollAME(N)
        setattr(self, f"threshE_{N}",rclim[self.ivar].data)
        thresh=getattr(self,f"threshE_{N}").repeat(365,axis=0) # this is correct use of repeat: individual array elements are repeated 365 times (filling out the year from an annual value)
        if self.exdir==1:
            setattr(self,f"is_exE_{N}",self.dat>thresh)
        elif self.exdir==-1:
            setattr(self,f"is_exE_{N}",self.dat<thresh)
        setattr(self,f"is_exE_{N}",zarrstash(getattr(self,f"is_exE_{N}").astype(int),f'is_exE_{N}',self.stmp).rechunk(self.chdef0.values()))
        setattr(self,f"is_exE_{N}_sum", getattr(self,f"is_exE_{N}").sum(axis=0).compute())
        idiff=da.diff(getattr(self,f"is_exE_{N}").astype(int),axis=0,prepend=0,append=0)
        out=da.apply_along_axis(eventcalcs,0,idiff,dtype=float,shape=(7,)).compute()
        setattr(self,f"exE_{N}_gt0_count",out[0,...])
        setattr(self,f"exE_{N}_gt5_count",out[1,...])
        setattr(self,f"exE_{N}_gt30_count",out[2,...])
        setattr(self,f"exE_{N}_gt180_count",out[3,...])
        setattr(self,f"exE_{N}_gt0_meandur",out[4,...])
        setattr(self,f"exE_{N}_gt5_meandur",out[5,...])
        setattr(self,f"exE_{N}_maxdur",out[6,...])
    def exF100_all(self,calcDurAmpF100=False):# like C but rolling window
        rt=self.yrspan[-1]-self.yrspan[0]+1
        self.threshF100=da.rechunk(da.tile(self.threshC,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)+self.delta_rollSeas100 # corrected to tile!
        thresh=self.threshF100
        #print('F100',type(thresh)) da.core.Array
        if self.exdir==1:
            self.is_exF100=self.dat>thresh
        elif self.exdir==-1:
            self.is_exF100=self.dat<thresh
        self.is_exF100_sum=self.is_exF100.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpF100:
            self.exF100_dur=da.apply_along_axis(meanlen,0,self.is_exF100).compute()
            self.exF100_amp=self.amps(thresh)
        #self.exF100_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.rechunk(thresh,chunks=self.chksz)),
        #                       axis=0).compute()
    def exG100_all(self,calcDurAmpG100=False):# like D but rolling window
        rt=self.yrspan[-1]-self.yrspan[0]+1
        self.threshG100=da.rechunk(da.tile(self.threshD,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)+self.delta_rollSeas100 # corrected to tile!

        thresh=self.threshG100
        print('G100',type(thresh)) # da.core.Array
        if self.exdir==1:
            self.is_exG100=self.dat>thresh
        elif self.exdir==-1:
            self.is_exG100=self.dat<thresh
        self.is_exG100_sum=self.is_exG100.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpG100:
            self.exG100_dur=da.apply_along_axis(meanlen,0,self.is_exG100).compute()
            self.exG100_amp=self.amps(thresh)
        #if type(thresh)==np.ndarray:
        #    self.exG100_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.from_array(thresh,chunks=self.chksz)),
        #                       axis=0).compute()
        #else:
        #    self.exG100_amp=da.nanmean(da.map_blocks(lambda aa,bb: getAmps(aa,bb,self.exdir),self.dat,
        #                                     da.rechunk(thresh,chunks=self.chksz)),
        #                       axis=0).compute()
    def exG_N_all(self,N):
        rt=self.yrspan[-1]-self.yrspan[0]+1
        setattr(self,f"threshG_{N}",da.rechunk(da.tile(self.threshD,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)+\
                                    getattr(self,f"delta_rollSeas_{N}")) # corrected to tile!
        thresh=getattr(self,f"threshG_{N}")
        print(f'G_{N}',type(thresh)) # da.core.Array
        if self.exdir==1:
            setattr(self,f"is_exG_{N}",self.dat>thresh)
        elif self.exdir==-1:
            setattr(self,f"is_exG_{N}",self.dat<thresh)
        setattr(self,f"is_exG_{N}",zarrstash(getattr(self,f"is_exG_{N}").astype(int),f'is_exG_{N}',self.stmp).rechunk(self.chdef0.values()))
        setattr(self,f"is_exG_{N}_sum",getattr(self,f"is_exG_{N}").sum(axis=0).compute())
        idiff=da.diff(getattr(self,f"is_exG_{N}").astype(int),axis=0,prepend=0,append=0)
        out=da.apply_along_axis(eventcalcs,0,idiff,dtype=float,shape=(7,)).compute()
        setattr(self,f"exG_{N}_gt0_count",out[0,...])
        setattr(self,f"exG_{N}_gt5_count",out[1,...])
        setattr(self,f"exG_{N}_gt30_count",out[2,...])
        setattr(self,f"exG_{N}_gt180_count",out[3,...])
        setattr(self,f"exG_{N}_gt0_meandur",out[4,...])
        setattr(self,f"exG_{N}_gt5_meandur",out[5,...])
        setattr(self,f"exG_{N}_maxdur",out[6,...])
    def exF60_all(self,calcDurAmpF60=False):# like C but rolling window
        rt=self.yrspan[-1]-self.yrspan[0]+1
        self.threshF60=da.rechunk(da.tile(self.threshC,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)+self.delta_rollSeas60 # corrected to tile!

        thresh=self.threshF60
        if self.exdir==1:
            self.is_exF60=self.dat>thresh
        elif self.exdir==-1:
            self.is_exF60=self.dat<thresh
        self.is_exF60_sum=self.is_exF60.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpF60:
            self.exF60_dur=da.apply_along_axis(meanlen,0,self.is_exF60).compute()
            self.exF60_amp=self.amps(thresh)

    def exG60_all(self,calcDurAmpG60=False):# like D but rolling window
        rt=self.yrspan[-1]-self.yrspan[0]+1
        self.threshG60=da.rechunk(da.tile(self.threshD,(rt,)+tuple(np.ones(len(self.chksz)-1,dtype=int))),self.chksz)+self.delta_rollSeas60 # corrected to tile!

        thresh=self.threshG60
        if self.exdir==1:
            self.is_exG60=self.dat>thresh
        elif self.exdir==-1:
            self.is_exG60=self.dat<thresh
        self.is_exG60_sum=self.is_exG60.sum(axis=0).compute()
        if self.calcDurAmp or calcDurAmpG60:
            self.exG60_dur=da.apply_along_axis(meanlen,0,self.is_exG60).compute()
            self.exG60_amp=self.amps(thresh)

    def exAbs_all(self,thresh,calcDurAmpAbs=False):# absolute, constant thresholds
        varstr=f"Abs_{thresh:.3f}".replace('.','_').replace('-','m')
        setattr(self,'thresh'+varstr,thresh)
        if self.exdir==1:
            setattr(self,'is_ex'+varstr,self.dat>thresh)
        elif self.exdir==-1:
            setattr(self,'is_ex'+varstr,self.dat<thresh)
        setattr(self,'is_ex'+varstr+'_sum',getattr(self,'is_ex'+varstr).sum(axis=0).compute())
        if self.calcDurAmp or calcDurAmpAbs:
            setattr(self,'ex'+varstr+'_dur',da.apply_along_axis(meanlen,0,getattr(self,'is_ex'+varstr)).compute())
            setattr(self,'ex'+varstr+'_amp',self.amps(da.expand_dims(np.array(thresh),axis=[0,1,2])))
    def reg_dp(self,dplist=None): # add extreme days per year and year-day
        if dplist is None:
            dplist=[el for el in vars(self).keys() if (el.startswith('is_ex') and not el.endswith('_sum'))]
        for iex in dplist:
            idpd,idpy=reg_dpd_dpy(getattr(self,iex))
            setattr(self,'dpd'+iex[2:],idpd)
            setattr(self,'dpy'+iex[2:],idpy)
    def save2d(self,savelist=None,mode='a'):
        # mode: 'a': existing variables overwritten; 'w': existing file overwritten
        saveloc=exstats_path(self.ivar,self.iscen,self.yrspan,self.refscen,self.refspan,self.freq,self.exdir,self.q,self.calcDurAmp)
        print(saveloc)
        vdict={}
        if savelist is None:
            savelist=[el for el in vars(self).keys() if (el.endswith('_sum') or el.endswith('dur') or el.endswith('count') or el.endswith('_amp'))]
        for el in savelist: # list of attributes corresponding to 2d variables
            vdict[el]=(['lat','lon'],getattr(self,el))
        dsout=xr.Dataset(data_vars=vdict,
                         coords=dict(lat=(["lat"],self.ds.lat.data),lon=(["lon"],self.ds.lon.data)),
                         attrs={'ivar':self.ivar,'freq':self.freq,'iscen':self.iscen,
                                'refscen':self.refscen,'yrspan':self.yrspan,'refspan':self.refspan})
        mkdirs(saveloc)
        dsout.to_netcdf(saveloc,mode=mode)
    def save_regts(self,savelist=None,mode='w'):
        saveloc=exstats_regts_path(self.ivar,self.iscen,self.yrspan,self.refscen,
                           self.refspan,self.freq,self.exdir,self.q,self.reg,self.calcDurAmp,grid='1x1')
        print(saveloc)
        vdict={}
        if savelist is None:
            savelist=[el for el in vars(self).keys() if ((el.startswith('is_ex') and not el.endswith('_sum')) or el.startswith('dpd_ex') or el.startswith('dpy_ex'))]
        for el in savelist: # list of attributes corresponding to 2d variables
            if el.startswith('is_ex'):
                vdict[el]=(['time','cellnum'],getattr(self,el))
            elif el.startswith('dpd'):
                vdict[el]=(['YD','cellnum'],getattr(self,el))
            elif el.startswith('dpy'):
                vdict[el]=(['year','cellnum'],getattr(self,el))
        dsout=xr.Dataset(data_vars=vdict,
                         coords=dict(time=(["time"],self.ds.time.data),
                                     YD=(["YD"],np.arange(1,366)),
                                     year=(["year"],np.arange(self.yrspan[0],self.yrspan[1]+1)),
                                     cellnum=(["cellnum"],self.ds.cellnum.data)),
                         attrs={'ivar':self.ivar,'freq':self.freq,'iscen':self.iscen,
                                'refscen':self.refscen,'yrspan':self.yrspan,
                                'refspan':self.refspan,
                               'region':self.reg,'j_indices':self.maskInfo.llats,
                                'i_indices':self.maskInfo.llons,
                               'sitename':self.maskInfo.sitename})
        mkdirs(saveloc)
        dsout.to_netcdf(saveloc)#,mode=mode)

    def printqs(self):# to double check that mean is not subtracted
        print(self.hstatsij['qt'].sel(quantile=self.q).values,
              self.hstatsij['qt_dt'].sel(quantile=self.q).values,
              self.hstatsij['qt_dtds'].sel(quantile=self.q).values)
    def closeall(self):
        try:
            self.hstats.close()
            del self.hstats
        except AttributeError:
            pass
        try:
            self.ds.close()
            del self.ds
        except AttributeError:
            pass
        try:
            self.fAME.close()
            del self.fAME
        except AttributeError:
            pass
        shutil.rmtree(self.stmp)

def calc_ex_multi(combolist,exlist,iscen,yrspan,scen0,yrspan0,freq,qq,calcDur=True,chdef0={'time':-1,'lat':90,'lon':90}):
    varlist=list({ix for iix in combolist for ix in iix}) # unique variables from combos
    segs={}# don't save between scenarios
    for ivar in varlist:
        print(ivar,dt.datetime.now())
        sf=xr.open_dataset(fNameStatsFile(scen0,yrspan0,freq,ivar))
        segs[(iscen,ivar)]=scenData(ivar,freq,iscen,chdef0)
        segs[(iscen,ivar)].setrefstats(sf,scen0,yrspan0)
        segs[(iscen,ivar)].setquant(qq)
        #print('setup:',np.shape(segs[(iscen,ivar)].dat),np.shape(segs[(iscen,ivar)].ds[ivar]))
        if 'is_exA' in exlist: segs[(iscen,ivar)].exA_all()
        if 'is_exC' in exlist: segs[(iscen,ivar)].exC_all()
        if 'is_exD' in exlist: segs[(iscen,ivar)].exD_all()
        if ('is_exE60' in exlist) or ('is_exE100' in exlist):
            segs[(iscen,ivar)].setAME(openAME(iscen,ivar,segs[(iscen,ivar)].exdir))
        if 'is_exE60' in exlist: segs[(iscen,ivar)].exE60_all()
        if 'is_exE100' in exlist: segs[(iscen,ivar)].exE100_all()
        print('done E')
        if ('is_exF60' in exlist) or ('is_exG60' in exlist):
            segs[(iscen,ivar)].rollSeas60()
        #print('done roll 60')
        if 'is_exF60' in exlist: segs[(iscen,ivar)].exF60_all()
        if 'is_exG60' in exlist: segs[(iscen,ivar)].exG60_all()
        if ('is_exF100' in exlist) or ('is_exG100' in exlist):
            segs[(iscen,ivar)].rollSeas100()
        #print('done roll 100')
        if 'is_exF100' in exlist: segs[(iscen,ivar)].exF100_all()
        if 'is_exG100' in exlist: segs[(iscen,ivar)].exG100_all()
    print('done load')
    for combo in combolist:
        vdict={};sums={};durs={};
        for iex in exlist:
            #initialize array of correct size as true
            iexout=da.full_like(getattr(segs[(iscen,combo[0])],iex),True)
            for ivar in combo:
                iexout=da.logical_and(iexout,getattr(segs[(iscen,ivar)],iex))
            vdict[iex+'_sum']=(['lat','lon'],iexout.sum(axis=0).compute())
            if calcDur:
                vdict[iex+'_dur']=(['lat','lon'],da.apply_along_axis(meanlen,0,iexout).compute())
        saveloc=exstats_multi_path(combo,iscen,yrspan,scen0,yrspan0,freq,qq,calcDur)
        print(saveloc)
        fex=segs[(iscen,combo[0])]
        dsout=xr.Dataset(data_vars=vdict,
                         coords=dict(lat=(["lat"],fex.ds.lat.data),lon=(["lon"],fex.ds.lon.data)),
                         attrs={'variables':combo,'freq':freq,'iscen':iscen,
                                'refscen':scen0,'yrspan':yrspan,'refspan':yrspan0,
                                'exdirs':', '.join([f"{iv}:{exdirs[iv]}" for iv in combo])})
        mkdirs(saveloc)
        dsout.to_netcdf(saveloc)
        del dsout
    for ivar in varlist:
        segs[(iscen,ivar)].closeall()
    return saveloc


def calc_MMM_NOAA(iscen0,yrspan0,targettime):
    # iscen0 is scenario, eg 'ESM4_historical_D1'
    # yrspan0 contains first and last year (inclusive), eg [1975,2014]
    # targettime is date used in trend correction
    # (tref is just a date for the time index)
    ivar='tos'
    freq='monthly'
    yrspan0=dictScenDates[iscen0]
    ylen=12
    tref=dt.datetime(yrspan0[0],1,1) # times referenced to start of series
    dat,time,dims,sattrs,ds = loadSlab(ivar,freq,iscen0,yrspan0,chunksdef)
    mod_tnl=ds.time.values # already on noleap calendar
    mod_val=np.array(ds['tos'])
    mod_tdt=cftnoleap_to_dt(mod_tnl) # datetimes for plotting
    mod_tind=to_exact_tind(mod_tnl,torig=tref)
    mod_ym=monthfromdt(mod_tdt)
    mod_yind=mod_ym
    seas0=binseas(mod_ym,mod_val,ylen)
    dsea=seas0.deseas(mod_ym,mod_val)
    lf=linfit(mod_tind,dsea)
    newvals=lf.targetdetrend(mod_tind,mod_val,targettime)
    seasF=binclim(mod_ym,newvals,ylen)
    MMM=np.max(seasF,0)
    # save:
    dataar=xr.DataArray(MMM,coords=(ds.lat,ds.lon),name='MMM',
                        attrs={'long_name':'Maximum Monthly Mean','units':'degC'})
    fname=MMM_NOAA_path(iscen0,yrspan0,targettime)
    dataar.to_netcdf(fname)
    return fname

def calcDHW(iscen,yrspan,iscenMMM,yrspanMMM,dtargetMMM=None,save=True):
    def _HS(T,MMM):
        if len(T.shape)==len(MMM.shape)+1:
            MMM=np.expand_dims(MMM,0)
        return T-MMM
    
    def _DHW(T,MMM,cumweeks=12):
        tdelt=cumweeks*7
        HS0=_HS(T,MMM)
        HSC=np.where(HS0>1,HS0,0) # count only when HS>1
        return np.array([np.sum(HSC[ii-tdelt:ii,...],0)/7 for ii in range(tdelt,T.shape[0]+1)])

    if dtargetMMM is None:
        dtargetMMM=targettime_MMM_NOAA
    ivar='tos'
    freq='daily'
    dat,time,dims,sattrs,ds = loadSlab(ivar,freq,iscen,yrspan,chunksdef)
    fMMM=xr.open_dataset(MMM_NOAA_path(iscenMMM,yrspanMMM,dtargetMMM))
    MMM=np.array(fMMM.MMM)
    # calculate:
    DHW1=_DHW(ds.tos,MMM)
    time=ds.time[12*7-1:].data
    dsout = xr.Dataset(data_vars=dict(DHW=(["time", "lat", "lon"], DHW1)),
                       coords=dict(lon=(["lon"],ds.lon.data),lat=(["lat"],ds.lat.data),time=(["time"],time)),
                       attrs=dict(description=f"Degree heating weeks: {iscen} with MMM based on {iscenMMM}"))
    fname=DHW_path(iscen,yrspan,iscenMMM,yrspanMMM,dtargetMMM)
    print(fname)
    if save:
        dsout.to_zarr(fname)
    return DHW1

def calc_abs_coral(iscen,yrspan):
    fDHW=xr.open_zarr(DHW_path(iscen,dictScenDates[iscen],
                                       'ESM4_historical_D1',dictScenDates['ESM4_historical_D1'],targettime_MMM_NOAA))
    ti=365*40-len(fDHW['time'])
    oms=scenData('omega_arag_0','daily',iscen,chunksdef)
    spcs=scenData('spco2','daily',iscen,chunksdef)
    dhwct=(fDHW['DHW'].values>4).sum(axis=0)/len(fDHW['time'])*100
    oms01ct=(oms.dat<1).sum(axis=0)/(365*40)*100
    om3ct=(oms.dat<3).sum(axis=0)/(365*40)*100
    omdhw3ct=da.logical_and(oms.dat[ti:,...]<3,fDHW['DHW'].values>4).sum(axis=0)/len(fDHW['time'])*100
    omdhw3ct_or=da.logical_or(oms.dat[ti:,...]<3,fDHW['DHW'].values>4).sum(axis=0)/len(fDHW['time'])*100
    # 1atm = 101325 Pa; convert 1000 mu-atm to Pa
    # 1000*1e-6 atm * 101325 Pa/1atm -> 
    spco2ct=(spcs.dat/101325*1e6>1000).sum(axis=0)/(365*40)*100
    fout=abs_coral_path(iscen,yrspan)
    dsout = xr.Dataset(data_vars=dict(dhw_pct=(["lat", "lon"],dhwct),
                                      om3_pct=(["lat", "lon"],om3ct),
                                      om3_and_dhw_pct=(["lat", "lon"],omdhw3ct),
                                      om3_or_dhw_pct=(["lat", "lon"],omdhw3ct_or),
                                      om1_pct=(["lat", "lon"],oms01ct),
                                      spc1000uatm_pct=(["lat","lon"],spco2ct),),
                       coords=dict(lon=(["lon"],fDHW.lon.data),lat=(["lat"],fDHW.lat.data)),
                       attrs=dict(description=f"reference scenario: ESM4_historical_D1, DHW reference date: 1988.286"))
    mkdirs(fout)
    dsout.to_netcdf(fout,mode='w')
    return fout

###################################################################

#############################################################################################################
if __name__ == '__main__':
    iscen=sys.argv[1]
    ivar=sys.argv[2]
    if not iscen in ('ESM4_historical_D1','ESM4_ssp126_D1','ESM4_ssp370_D1'):
        raise Exception(f"option '{iscen}' not available")
    if not ivar in ('tos','phos','omega_arag_0','spco2','hplusos'):
        raise Exception(f"option '{ivar}' not available")
    do_save_omega=False
    do_run_calcs=False
    do_calc_AME=False
    do_rollSeas=False
    do_annex=True
    do_scenData=False
    do_scenWindows=False
    do_calc_ex_multi=False
    do_MMM_NOAA=False
    do_calcDHW=False
    do_abs_coral=False
    if ivar=='omega_arag_0' and do_save_omega:
        for iscen in ('ESM4_historical_D1','ESM4_ssp126_D1','ESM4_ssp370_D1'):
            yrspan=scenDefaults[iscen][0]
            freq='daily'
            print(save_omega_arag_0(iscen,yrspan,freq))
            yrspan=yrspanM[iscen]
            freq='monthly'
            print(save_omega_arag_0(iscen,yrspan,freq))
    if do_run_calcs:
        cluster = LocalCluster(n_workers=5,threads_per_worker=1,)
        client = Client(cluster)
        run_calcs(ivar,'daily',iscen,scenDefaults[iscen][0],recalc=True,ydqtcalc=True if iscen=='ESM4_historical_D1' else False)
    if do_calc_AME:
        vlistmax=['tos','spco2','hplusos',]#'thetao_50','tos','hplus_50']
        vlistmin=['omega_arag_0']#,'omega_arag_50']#'phos','o2_mean_100_600','o2_50','ph_50','omega_arag_0','omega_arag_50']
        if ivar in vlistmax:
            print(ivar,'max')
            path=calc_AME(ivar,iscen,1)
        elif ivar in vlistmin:
            print(ivar,'min')
            path=calc_AME(ivar,iscen,-1)
    if do_rollSeas: # only req 1 processor
        for window in [10,30,50,75,100,125]:
            seg=scenData(ivar,'daily',iscen,{'time':-1,'lat':90,'lon':90},False)
            seg.refscen='ESM4_historical_D1'
            seg.refspan=[1975,2014]
            seg.rollSeas(window)
    if do_annex:
        yrspan=scenDefaults[iscen][0]
        cluster = LocalCluster(n_workers=5,threads_per_worker=1,)
        client = Client(cluster)
        run_annual_extremes(ivar,'daily',iscen,yrspan,recalc=False)
        client.close()
        cluster.close()
    for qq in [.95,]: # .93,.95,.98
        if do_scenData:
            calcDurAmp=False
            # qq=.95
            scenList=[iscen,]
            freq='daily'
            chdef0={'time':-1,'lat':90,'lon':180}
            scen0='ESM4_historical_D1'
            yrspan0=dictScenDates[scen0]
            for el in scenList:
                print(exstats_path(ivar,el,dictScenDates[el],scen0,dictScenDates[scen0],freq,exdirs[ivar],qq if exdirs[ivar]==1 else np.round(1-qq,2)))
                if os.path.exists(exstats_path(ivar,el,dictScenDates[el],scen0,dictScenDates[scen0],freq,exdirs[ivar],qq if exdirs[ivar]==1 else np.round(1-qq,2))):
                    continue #already done
                cluster = LocalCluster(n_workers=5,threads_per_worker=1,)
                client = Client(cluster)
                print(el,ivar,dt.datetime.now())
                sf=xr.open_dataset(fNameStatsFile(scen0,yrspan0,freq,ivar)) # open here in case closed by closeall() below
                seg=scenData(ivar,freq,el,chdef0,calcDurAmp=calcDurAmp)
                seg.setrefstats(sf,scen0,yrspan0)
                seg.setquant(qq)
                seg.exA_all()
                seg.exC_all()
                seg.exD_all()
                seg.setAME(openAME(el,ivar,seg.exdir))
                #seg.exE60_all()
                seg.exE100_all()
                #print('60s',dt.datetime.now())
                print('100s',dt.datetime.now())
                seg.rollSeas100()
                seg.exF100_all()
                seg.exG100_all()
                if 'omega' in ivar:
                    seg.exAbs_all(1.0)
                    seg.exAbs_all(2.0)
                    seg.exAbs_all(3.0)
                elif ivar=='spco2':
                    seg.exAbs_all(1000*101325*1e-6)
                print('save',dt.datetime.now())
                sys.stdout.flush()
                seg.save2d()
                seg.closeall()
                del seg
                print('loop end')
                sys.stdout.flush()
                client.close()
                cluster.close()
        if do_calc_ex_multi and not ivar=='tos':
            yrspan=dictScenDates[iscen]
            # qq=.95
            freq='daily'
            chdef0={'time':-1,'lat':90,'lon':90}
            scen0='ESM4_historical_D1'
            yrspan0=dictScenDates[scen0]
            combolist=[('tos',ivar),] # list of tuples
            exlist=['is_exA','is_exC','is_exD','is_exE100','is_exF100','is_exG100']
            calc_ex_multi(combolist,exlist,iscen,yrspan,scen0,yrspan0,freq,qq,calcDur=False,chdef0=chdef0)
            print('done',dt.datetime.now())
    if do_scenWindows:
        freq='daily'
        qq=.95
        scen0='ESM4_historical_D1'
        yrspan0=dictScenDates[scen0]
        chdef0={'time':-1,'lat':90,'lon':90}
        fname=exstats_path(ivar,iscen,dictScenDates[iscen],scen0,dictScenDates[scen0],freq,exdirs[ivar],qq if exdirs[ivar]==1 else np.round(1-qq,2))
        for window in [10,30,50,75,100,125]:
            #if os.path.exists(fname) and nchasvar(f'exE_{window}_maxdur',fname) and nchasvar(f'exG_{window}_maxdur',fname):
            #    continue #already done
            cluster = LocalCluster(n_workers=5,threads_per_worker=1,)
            client = Client(cluster)
            sf=xr.open_dataset(fNameStatsFile(scen0,yrspan0,freq,ivar)) # open here in case closed by closeall() below
            seg=scenData(ivar,freq,iscen,chdef0)
            seg.setrefstats(sf,scen0,yrspan0)
            seg.setquant(qq)
            seg.setAME(openAME(iscen,ivar,seg.exdir))
            #if not nchasvar(f'exE_{window}_maxdur',fname): 
            seg.exE_N_all(window)
            seg.save2d()
            #if not nchasvar(f'exG_{window}_maxdur',fname): 
            seg.exD_all()
            seg.rollSeas_N(window)
            seg.exG_N_all(window)
            seg.save2d()

            seg.closeall()
            client.close()
            cluster.close()
    if do_MMM_NOAA and ivar=='tos' and iscen=='ESM4_historical_D1':
        fp=calc_MMM_NOAA(iscen,dictScenDates[iscen],targettime_MMM_NOAA)
    if do_calcDHW and ivar=='tos':
        iscenMMM='ESM4_historical_D1'
        dhw=calcDHW(iscen,dictScenDates[iscen],iscenMMM,dictScenDates[iscenMMM],dtargetMMM=None,save=True)
    if do_abs_coral and ivar=='omega_arag_0': 
        f=calc_abs_coral(iscen,dictScenDates[iscen])
    print('done')

