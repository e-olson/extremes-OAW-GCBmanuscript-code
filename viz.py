import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import netCDF4 as nc
import xarray as xr
import socket
import matplotlib.ticker as mtick
import matplotlib.colors as mcolors
import cmocean
from extremespaper import sourcepath

ALoc=mtick.MaxNLocator(nbins='auto',steps=[1,2,2.5,3,5,10])

## define colormaps ##
# colors1 = cmocean.cm.thermal(np.linspace(.1, 1, 100))
# colors2 = np.zeros((1,4))
# colors2[:,-1]=1.
# cm1b = mcolors.LinearSegmentedColormap.from_list('kzerotherm',  np.vstack((colors2, colors1)))
# cm1b.set_bad('w',alpha=0)
# cm1=mcolors.LinearSegmentedColormap.from_list('kzerotherm2',  colors1)
# cm1.set_bad('w',alpha=0)
# cm1.set_under('k')
colors1 = cmocean.cm.thermal_r(np.linspace(.1, 1, 100))# matter?
for i in range(0,10):
    colors1[i,1]=min(1,colors1[i,1]+.15*(10-i)/10)
colors2 = np.ones((1,4))
colors2[:,-1]=0. #1.
cm1b = mcolors.LinearSegmentedColormap.from_list('kzerotherm',  np.vstack((colors2, colors1)))
cm1b.set_bad('w',alpha=0)
colors2b = np.ones((2,4))
colors2b[:,-1]=0. #1.
cm2b = mcolors.LinearSegmentedColormap.from_list('kzerotherm',  np.vstack((colors2b, colors1)))
cm2b.set_bad('w',alpha=0)
cm1=mcolors.LinearSegmentedColormap.from_list('kzerotherm2',  colors1)
cm1.set_bad('gray',alpha=0)
cm1.set_under('w')
cm2=mcolors.LinearSegmentedColormap.from_list('kzerotherm2',  colors1)
cm2.set_bad('w',alpha=0)
cm2.set_under('lightgray')
cm2.set_over('gray')
cmb=cmocean.cm.balance
cmb.set_bad('w',alpha=0)


fex=f'{sourcepath}ESGF/ESM4_historical_D1/tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_19700101-19791231.nc'
with nc.Dataset(fex) as fstat:
    lonvec1x1=fstat.variables['lon'][:]
    latvec1x1=fstat.variables['lat'][:]
    
fstatic1x1=f'{sourcepath}ge_data/ESM4_historical_D1/ocean_daily_1x1deg.static.nc'
with nc.Dataset(fstatic1x1) as fstat:
    glon1x1=fstat.variables['geolon'][:]
    glat1x1=fstat.variables['geolat'][:]
    deptho1x1=fstat.variables['deptho'][:]
    wet1x1=np.where(fstat.variables['wet'][:].mask,0.0,fstat.variables['wet'][:])
    areacello1x1=fstat.variables['areacello'][:]

def squareax(ax,line=True,origin=False,linespecs=None,eqlabels=True):
    if linespecs is None:
        linespecs={'linestyle':'-','color':'lightgray'}
    xl=ax.get_xlim()
    yl=ax.get_ylim()
    al=[min(0 if origin else xl[0],0 if origin else yl[0]),max(xl[1],yl[1])]
    if line:
        ax.plot(al,al,**linespecs)
    ax.set_xlim(al)
    ax.set_ylim(al)
    ax.set_aspect(1)
    if eqlabels:
        ticks=ax.get_xticks()
        ax.set_yticks(ticks)
        ax.set_xticks(ticks)
        ax.set_xlim(al)
        ax.set_ylim(al)
    return 

def gfdlLon(lon):
    """ Function to convert arbitrary longitude into the range used by gfdl model grid,
    for display purposes. Input can be a single longitude or array/list/dataarray of values. """
    if hasattr(lon,"__len__"): # assume array-like
        lontype=type(lon)
        newlon=[gfdlLon(el) for el in lon]
        if lontype==np.ndarray:
            return np.array(newlon)
        elif lontype==pd.core.series.Series:
            return pd.core.series.Series(newlon,index=lon.index)
        else:
            return newlon
    else:
        if lon<-300:
            return lon+360
        elif lon>60:
            return lon-360
        else:
            return lon

def makeline():
    fig,ax=plt.subplots(1,1,figsize=(12,.1))
    ax.plot([0,1],[0,0],'k-')
    ax.set_xlim(0,1)
    ax.axis('off' )
    return

def ax_repl(fig,ax): # replace axes projection axes with normal axes
    pos=ax.get_position()
    ax.remove()
    ax=fig.add_axes(pos.bounds)#[0],0.1,.01,0.8
    return ax
