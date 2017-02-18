		select t.*,
		 vol_con_ppp / sum(vol_con_ppp) over (partition by date_ts, hour, region_fk) as k
		from (
			select 
				
				dh.date as date_ts,
				dh.hour,
				dpg.REGION_FK,
				dpg.dpg_code,
				dpg.dpg_pk,
				sum( dh.vol_accepted_step_calc ) as vol_con_ppp
			from
				dds.DPGC_NODE_STEP_DATE_HOUR dh 
				inner join 
				dds.dpg dpg 
				on 
					dpg.DPG_PK = dh.DPGC_PK_FK 
					and dh.date between dpg.BEGIN_DATE and dpg.END_DATE 

				inner join 
				dds.dpgc dpgc
				on 
					dpgc.DPGC_PK_FK = dh.DPGC_PK_FK
					and dh.date between dpgc.BEGIN_DATE and dpgc.END_DATE  
					and dpgc.IS_AUX_DPG<>1
			where
				date in (select	date_ts	from dm_opr.model_scenarios where scenario_pk in (:scenario_id))
				
			group by 
				1,2,3,4,5
			order by 
				1,2,3,4,5
			) t