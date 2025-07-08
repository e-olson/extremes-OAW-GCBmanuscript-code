import geopandas as gpd
import numpy as np
import cartopy.crs as ccrs
import shapely
from shapely import geometry as geo
import dask_geopandas as dgpd
import xarray as xr
import viz

# load model grid
glon,glat,gmask=viz.glon1x1.astype(float),viz.glat1x1.astype(float),np.ceil(viz.wet1x1)
# set grid lon range to match geo dataset range of (-180,180)
glonR=np.ma.masked_where(np.ma.getmask(glon),np.where(glon<=-180,glon+360.,glon))

################ make coral mask ####################
gdbloc='/net2/ebo/obs/coralSites/14_001_WCMC008_CoralReefs2021_v4_1/01_Data/'
layers = fiona.listlayers(gdbloc)

gdf = {}
for el in layers:
    gdf[el] = gpd.read_file(gdbloc,layer=el)

crsPCinit=ccrs.PlateCarree(central_longitude=0).proj4_init

#### choice on how to handle points versus polygons:
# - layer 0 contains points associated with coral in Mexico without information on area EXCLUDE IT
# - layer 1 contains polygons associated with coral (reefs)
# - some grid cells contain both points and polygons
# - for boolean mask, set grid  cell to True if it contains any part of a polygon; 

projge1=gdf['WCMC008_CoralReef2021_Py_v4_1'].to_crs(crsPCinit)
dprojge1=dgpd.from_geopandas(projge1, npartitions=4)

# leave area empty; not used
locmask1=np.full_like(glonR.data,False,dtype=bool)
locArea1=np.nan*np.ones(np.shape(glonR.data))
bds=dprojge1.total_bounds.compute()
ply=shapely.Polygon([(bds[0],bds[1]),(bds[2],bds[1]),(bds[2],bds[3]),
                                 (bds[0],bds[3]),(bds[0],bds[1])])
iii=0
with np.nditer(locmask1, flags=['multi_index'], op_flags=['writeonly']) as it:
    for el in it:
        ilon=np.floor(glonR[it.multi_index])
        ilat=np.floor(glat[it.multi_index])
        if not np.ma.is_masked(ilon):
            #print(ilon,ilat,iii)
            iii+=1
            sq=shapely.Polygon([(ilon,ilat),(ilon+1,ilat),(ilon+1,ilat+1),
                                                                (ilon,ilat+1),(ilon,ilat)])
            if sq.intersects(ply): # speed up by ignoring cells outside max bounds
                for ind, row in projge1.iterrows():
                    if row.geometry.within(sq):
                        print(iii,'A')
                        el[...]=True
                        #locArea1[it.multi_index]=locArea1[it.multi_index]+row.GIS_AREA_K
                    elif row.geometry.intersects(sq):#row.geometry.overlaps(sq)|row.geometry.contains(sq):
                        print(iii,'B')
                        el[...]=True
                        #break
                        #AA=x.intersection(sq).to_crs(epsg=6933).area/1e6 # sq km
                        #locArea1[it.multi_index]=locArea1[it.multi_index]+AA
locmask=locmask1                 

locArea1=np.nan*locmask #hack because area not working

xv=np.concatenate([np.expand_dims(locmask,0),np.expand_dims(locArea1,0)],axis=0)
np.shape(xv)
dsout = xr.Dataset(data_vars=dict(coralmask=(["maskType", "lat", "lon"], xv)),
                   coords=dict(lon=(["lon"],viz.lonvec1x1),lat=(["lat"],viz.latvec1x1),maskType=(["maskType"],['Boolean','area'])),
                   attrs=dict(description=f"1x1 degree grid masks based on WCMC008 warm-water coral reef database"))
fname=f'/home/Elise.Olson/coralMasks.nc'
#mkdirs(fname)
dsout.to_netcdf(fname);


######## MPA masks ################
gdf=gpd.read_file(gdbloc,layer='NOAA_MPA_Inventory')
siteList=list(ex.siteDict.values())[:-1]

gdbsel=gdf.loc[gdf.Site_Name.isin(siteList)]

def makemask(gdfentry,a=0.01,b=0.001):
    # given a geopandas row gdfentry, return mask on 1x1 grid where True values represent overlap area
    # between shape and cell that is > a*100% of total shape area or >b*100% of cell area
    # first, simplify geometry to reduce processing time
    projge=gdfentry.to_crs(crsPCinit).geometry.buffer(.001).simplify(.001)
    print('Number of points defining simplified geometry:',len(projge.geometry.get_coordinates()))
    locmask=np.full_like(glonR.data,False,dtype=bool)
    with np.nditer(locmask, flags=['multi_index'], op_flags=['writeonly']) as it:
        for el in it:
            ilon=np.floor(glonR[it.multi_index])
            ilat=np.floor(glat[it.multi_index])
            if not np.ma.is_masked(ilon):
                sq=shapely.Polygon([(ilon,ilat),(ilon+1,ilat),(ilon+1,ilat+1),
                                                                    (ilon,ilat+1),(ilon,ilat)])
                if projge.geometry.item().overlaps(sq)|projge.geometry.item().contains(sq):
                    Ao=projge.geometry.item().intersection(sq).area
                    A1=projge.geometry.item().area
                    if (Ao/A1 > a) | (Ao > b): # >10% of total MPA area in cell or >1% of cell is in MPA
                        el[...]=True
    return locmask
    
def docalcmask(sitename,gdf):
    ig=gdf.loc[gdf.Site_Name==sitename].to_crs(ccrs.PlateCarree(central_longitude=0).proj4_init)
    return makemask(ig)

masks={}
for iloc in siteList:
    masks[iloc]=.docalcmask(iloc,gdf)

xx=[]
for el in siteList:
    xx.append(np.expand_dims(masks[el],0))
xv=np.concatenate(xx)
np.shape(xv)

dsout = xr.Dataset(data_vars=dict(MPAmask=(["MPA", "lat", "lon"], xv)),
                   coords=dict(lon=(["lon"],viz.lonvec1x1),lat=(["lat"],viz.latvec1x1),MPA=(["MPA"],siteList)),
                   attrs=dict(description=f"1x1 degree grid mask for selected MPAs"))
fname=f'/work/ebo/calcs/extremes/MPAsites/siteMasksNew.nc'
cf.mkdirs(fname)
print(fname)
dsout.to_netcdf(fname);
