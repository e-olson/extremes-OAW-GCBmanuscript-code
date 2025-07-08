# extremes-OAW-GCBmanuscript-code
This repository contains analysis code accompanying the manuscript, "Potential for regional resilience to ocean warming and acidification extremes: Projected vulnerability under contrasting pathways and thresholds" by Elise M. B. Olson, Jasmin G. John, John Dunne, Charles Stock, and Elizabeth J. Drenkard. 

The figures in the manusccript were created with the Jupyter Notebooks (*.ipynb) in the "notebooks" subdirectory.
Figure...File Name...............................................Notebook
1........global_extmes_seas_All_newcm.png........................001r_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
2........global_extmes_seas_Multi_All_newcm.png..................003_map_EventDays-Figs-Simple-Multi-Paper-newCmap.ipynb
3........MMMm_All_sll.png........................................010r_MMMm-plots-Paper.ipynb
4........global_extmes_seasAdapt_All_newcm.png...................001r_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
5........global_extmes_seasAdapt_Multi_All_newcm.png.............003_map_EventDays-Figs-Simple-Multi-Paper-newCmap.ipynb
6........global_extmes_coral_newcm.png...........................004r_coralExtremes-newCmap.ipynb
7........multistrescoral_newcm.png...............................004r_coralExtremes-newCmap.ipynb
8........distrib_p9ii.png, deltas.png............................006_extremes-spread-T-omega-Corrected-updated.ipynb
9........sites_MPAs.png..........................................011_map-sites.ipynb
10.......MPASitesBars.png........................................007rr_ExEvents-MPA-dev-bars-PaperFig-spco2Version.ipynb
11.......MPA_extremes_coral.png..................................004r_coralExtremes-newCmap.ipynb
S1.......percentileVsMMMm.png....................................008r_AMMvsP95-Fig.ipynb
S2.......ptileMatch.png..........................................008r_AMMvsP95-Fig.ipynb
S3.......thresholds.png..........................................thresholdIllustration.ipynb
S4.......global_extremes_seas_All_newcm_93.png, .................001r_93_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
         global_extremes_seas_All_newcm_98.png...................001r_98_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
S5.......global_extremes_seas_All_newcm_hplus.png................001r_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
S6.......global_extremes_seas_tos_diff_newcm.png.................002_map_EventDays-Figs-many-newCmap.ipynb
S7.......global_extremes_seas_spco2_diff_newcm.png, .............002_map_EventDays-Figs-many-newCmap.ipynb
         global_extremes_seas_omega_arag_0_diff_newcm.png........002_map_EventDays-Figs-many-newCmap.ipynb
S8.......global_extremes_seasAdapt_All_newcm_hplusos.png.........001r_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
S9.......global_extremes_seasAdapt50_All_newcm.png...............001r_map_EventDays-Figs-Simple-Combined-newCmap.ipynb
S10......global_extremes_seasAdapt_tos_diff_newcm.png............002_map_EventDays-Figs-many-newCmap.ipynb
S11......global_extremes_seasAdapt_hplusos_diff_newcm.png........002_map_EventDays-Figs-many-newCmap.ipynb
         global_extremes_seasAdapt_omega_arag_0_diff_newcm.png...002_map_EventDays-Figs-many-newCmap.ipynb
S12......delta_mean.png..........................................009r_extremes-95th-compare-paperfig-202408-Corrected-Edit-Means.ipynb
S13......delta_95mean_ratio.png..................................009r_extremes-95th-compare-paperfig-202408-Corrected-Edit-Means.ipynb
S14......distrib_Mean_ii.png.....................................009r_extremes-95th-compare-paperfig-202408-Corrected-Edit-Means.ipynb
S15......MPASitesBarsFixedHRef.png...............................007rr_ExEvents-MPA-dev-bars-PaperFig-spco2Version.ipynb
S16......MPASitesBarsDeltas.png..................................009r_extremes-95th-compare-paperfig-202408-Corrected-Edit-Means.ipynb
	
The main analysis code is contained in extremes.py and was run through calls to that script with the arguments $iscen and $ivar:
python extremespaper.py $iscen $ivar
where iscen is the scenario, 'ESM4_historical_D1','ESM4_ssp126_D1', or 'ESM4_ssp370_D1', and ivar is the variable name, 'tos','phos','omega_arag_0','spco2', or 'hplusos'.
To carry out various calculations, flags in lines 1462-1472 were set to True or False. 
The paths "sourcepath", "workpath", and "tmpdir" must be defined (lines 27-29), and the directory "sourcepath" contains the model output data files in the following structure:

├── ESGF
│   ├── ESM4_historical_D1
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_185001-186912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_187001-188912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_189001-190912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_191001-192912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_193001-194912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_195001-196912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_197001-198912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_199001-200912.nc
│   │   ├── co3os_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_201001-201412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_185001-186912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_187001-188912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_189001-190912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_191001-192912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_193001-194912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_195001-196912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_197001-198912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_199001-200912.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_201001-201412.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_185001-186912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_187001-188912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_189001-190912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_191001-192912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_193001-194912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_195001-196912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_197001-198912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_199001-200912.nc
│   │   ├── phos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_201001-201412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_185001-186912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_187001-188912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_189001-190912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_191001-192912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_193001-194912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_195001-196912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_197001-198912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_199001-200912.nc
│   │   ├── spco2_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_201001-201412.nc
│   │   ├── tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_19700101-19791231.nc
│   │   ├── tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_19800101-19891231.nc
│   │   ├── tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_19900101-19991231.nc
│   │   ├── tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_20000101-20091231.nc
│   │   ├── tos_Oday_GFDL-ESM4_historical_r1i1p1f1_gr_20100101-20141231.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_185001-186912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_187001-188912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_189001-190912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_191001-192912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_193001-194912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_195001-196912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_197001-198912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_199001-200912.nc
│   │   ├── tos_Omon_GFDL-ESM4_historical_r1i1p1f1_gr_201001-201412.nc
│   ├── ESM4_ssp119_D1
│   │   ├── co3os_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_209501-210012.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_209501-210012.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_201501-203412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_203501-205412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_205501-207412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_207501-209412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_209501-210012.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_201501-203412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_203501-205412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_205501-207412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_207501-209412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_209501-210012.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_201501-203412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_203501-205412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_205501-207412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_207501-209412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp119_r1i1p1f1_gr_209501-210012.nc
│   ├── ESM4_ssp126_D1
│   │   ├── co3os_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_209501-210012.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_209501-210012.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_201501-203412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_203501-205412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_205501-207412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_207501-209412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_209501-210012.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_201501-203412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_203501-205412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_205501-207412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_207501-209412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_209501-210012.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp126_r1i1p1f1_gr_20550101-20641231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp126_r1i1p1f1_gr_20650101-20741231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp126_r1i1p1f1_gr_20750101-20841231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp126_r1i1p1f1_gr_20850101-20941231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp126_r1i1p1f1_gr_20950101-21001231.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_201501-203412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_203501-205412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_205501-207412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_207501-209412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp126_r1i1p1f1_gr_209501-210012.nc
│   ├── ESM4_ssp245_D1
│   │   ├── co3os_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_209501-210012.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_209501-210012.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_201501-203412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_203501-205412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_205501-207412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_207501-209412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_209501-210012.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_201501-203412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_203501-205412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_205501-207412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_207501-209412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_209501-210012.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_201501-203412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_203501-205412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_205501-207412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_207501-209412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp245_r1i1p1f1_gr_209501-210012.nc
│   ├── ESM4_ssp370_D1
│   │   ├── co3os_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_209501-210012.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_209501-210012.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_201501-203412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_203501-205412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_205501-207412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_207501-209412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_209501-210012.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_201501-203412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_203501-205412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_205501-207412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_207501-209412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_209501-210012.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp370_r1i1p1f1_gr_20550101-20641231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp370_r1i1p1f1_gr_20650101-20741231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp370_r1i1p1f1_gr_20750101-20841231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp370_r1i1p1f1_gr_20850101-20941231.nc
│   │   ├── tos_Oday_GFDL-ESM4_ssp370_r1i1p1f1_gr_20950101-21001231.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_201501-203412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_203501-205412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_205501-207412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_207501-209412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp370_r1i1p1f1_gr_209501-210012.nc
│   ├── ESM4_ssp585_D1
│   │   ├── co3os_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3os_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_209501-210012.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_201501-203412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_203501-205412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_205501-207412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_207501-209412.nc
│   │   ├── co3sataragos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_209501-210012.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_201501-203412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_203501-205412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_205501-207412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_207501-209412.nc
│   │   ├── phos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_209501-210012.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_201501-203412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_203501-205412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_205501-207412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_207501-209412.nc
│   │   ├── spco2_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_209501-210012.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_201501-203412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_203501-205412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_205501-207412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_207501-209412.nc
│   │   ├── tos_Omon_GFDL-ESM4_ssp585_r1i1p1f1_gr_209501-210012.nc
├── ge_data
│   ├── ESM4_historical_D1
│   │   └── ocean_daily_1x1deg.static.nc
│   ├── ESM4_historical_D1_CRT_1975_2014
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19750101-19791231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19750101-19791231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19800101-19841231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19800101-19841231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19850101-19891231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19850101-19891231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19900101-19941231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19900101-19941231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19950101-19991231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.19950101-19991231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20000101-20041231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20000101-20041231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20050101-20091231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20050101-20091231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20100101-20141231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20100101-20141231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19750101-19791231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19750101-19791231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19800101-19841231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19800101-19841231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19850101-19891231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19850101-19891231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19900101-19941231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19900101-19941231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19950101-19991231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.19950101-19991231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20000101-20041231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20000101-20041231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20050101-20091231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20050101-20091231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20100101-20141231.phos.nc
│   │   └── ocean_cobalt_daily_sfc_1x1deg.20100101-20141231.spco2.nc
│   ├── ESM4_ssp126_D1_CRT_2061_2100
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20610101-20651231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20610101-20651231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20660101-20701231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20660101-20701231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20710101-20751231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20710101-20751231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20760101-20801231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20760101-20801231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20810101-20851231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20810101-20851231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20860101-20901231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20860101-20901231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20910101-20951231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20910101-20951231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20960101-21001231.co3_0.nc
│   │   ├── ocean_cobalt_daily_sat_z_1x1deg.20960101-21001231.co3satarag_0.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20610101-20651231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20610101-20651231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20660101-20701231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20660101-20701231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20710101-20751231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20710101-20751231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20760101-20801231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20760101-20801231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20810101-20851231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20810101-20851231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20860101-20901231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20860101-20901231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20910101-20951231.phos.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20910101-20951231.spco2.nc
│   │   ├── ocean_cobalt_daily_sfc_1x1deg.20960101-21001231.phos.nc
│   │   └── ocean_cobalt_daily_sfc_1x1deg.20960101-21001231.spco2.nc
│   └── ESM4_ssp370_D1_CRT_2061_2100
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20610101-20651231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20610101-20651231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20660101-20701231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20660101-20701231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20710101-20751231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20710101-20751231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20760101-20801231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20760101-20801231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20810101-20851231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20810101-20851231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20860101-20901231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20860101-20901231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20910101-20951231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20910101-20951231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20960101-21001231.co3_0.nc
│       ├── ocean_cobalt_daily_sat_z_1x1deg.20960101-21001231.co3satarag_0.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20610101-20651231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20610101-20651231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20660101-20701231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20660101-20701231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20710101-20751231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20710101-20751231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20760101-20801231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20760101-20801231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20810101-20851231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20810101-20851231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20860101-20901231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20860101-20901231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20910101-20951231.phos.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20910101-20951231.spco2.nc
│       ├── ocean_cobalt_daily_sfc_1x1deg.20960101-21001231.phos.nc
│       └── ocean_cobalt_daily_sfc_1x1deg.20960101-21001231.spco2.nc
└── masks
    ├── coralMasks.nc
    └── siteMasksNew.nc

The data under "ESGF" is available here: 
https://esgf-metagrid.cloud.dkrz.de/search/cmip6-dkrz/
Citations:
John, Jasmin G; Blanton, Chris; McHugh, Colleen; Radhakrishnan, Aparna; Rand, Kristopher; Vahlenkamp, Hans; Wilson, Chandin; Zadeh, Niki T.; Dunne, John P.; Dussin, Raphael; Horowitz, Larry W.; Krasting, John P.; Lin, Pu; Malyshev, Sergey; Naik, Vaishali; Ploshay, Jeffrey; Shevliakova, Elena; Silvers, Levi; Stock, Charles; Winton, Michael; Zeng, Yujin (2018). NOAA-GFDL GFDL-ESM4 model output prepared for CMIP6 ScenarioMIP. Version YYYYMMDD[1].Earth System Grid Federation. https://doi.org/10.22033/ESGF/CMIP6.1414

Krasting, John P.; John, Jasmin G; Blanton, Chris; McHugh, Colleen; Nikonov, Serguei; Radhakrishnan, Aparna; Rand, Kristopher; Zadeh, Niki T.; Balaji, V; Durachta, Jeff; Dupuis, Christopher; Menzel, Raymond; Robinson, Thomas; Underwood, Seth; Vahlenkamp, Hans; Dunne, Krista A.; Gauthier, Paul PG; Ginoux, Paul; Griffies, Stephen M.; Hallberg, Robert; Harrison, Matthew; Hurlin, William; Malyshev, Sergey; Naik, Vaishali; Paulot, Fabien; Paynter, David J; Ploshay, Jeffrey; Reichl, Brandon G; Schwarzkopf, Daniel M; Seman, Charles J; Silvers, Levi; Wyman, Bruce; Zeng, Yujin; Adcroft, Alistair; Dunne, John P.; Dussin, Raphael; Guo, Huan; He, Jian; Held, Isaac M; Horowitz, Larry W.; Lin, Pu; Milly, P.C.D; Shevliakova, Elena; Stock, Charles; Winton, Michael; Wittenberg, Andrew T.; Xie, Yuanyu; Zhao, Ming (2018). NOAA-GFDL GFDL-ESM4 model output prepared for CMIP6 CMIP historical. Version YYYYMMDD[1].Earth System Grid Federation. https://doi.org/10.22033/ESGF/CMIP6.8597

The data under "ge_data" is available here: 
https://doi.org/10.25921/776n-5w58
Citation:
Olson, Elise M.; John, Jasmin G.; Dunne, John P.; Drenkard, Elizabeth; Stock, Charles A. (2025). Data Accompanying Manuscript "Potential for regional resilience to ocean warming and acidification extremes: Projected vulnerability under contrasting pathways and thresholds" from 1975-01-01 to 2100-12-31 (NCEI Accession 0305815). NOAA National Centers for Environmental Information. Dataset. https://doi.org/10.25921/776n-5w58. 

The code to create the files in the "masks" directory, coralMasks.nc and siteMasksNew.nc, is in make_masks.py and requires geospatial data from the coral reef and MPA databases available here:
coral: https://data-gis.unep-wcmc.org/portal/home/item.html?id=0613604367334836863f5c0c10e452bf
MPAs: https://www.fisheries.noaa.gov/inport/item/69506

Citations:
UNEP-WCMC, WorldFish Centre, WRI, TNC (2021). Global distribution of warm-water coral reefs, compiled from multiple sources including the Millennium Coral Reef Mapping Project. Version 4.1. Includes contributions from IMaRS-USF and IRD (2005), IMaRS- USF (2005) and Spalding et al. (2001). Cambridge (UK): UN Environment World Con- servation Monitoring Centre. Data DOI: https://doi.org/10.34892/t2wk-5t34.

Office of National Marine Sanctuaries (2023). Marine Protected Areas (MPA) Inventory 2023-2024. NOAA National Centers for Environmental Information, USA. Data set accessed 2023-10-23 at https://www.fisheries.noaa.gov/inport/item/69506.
