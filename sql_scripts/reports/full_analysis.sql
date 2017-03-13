select 'СИПР' type, c.target_date, c.price_zone,
c.tg_cons, c.pr_cons, c.tg_gen, c.pr_gen, c.bid_volume_less_650,
c.bid_volume_less_800, c.bid_volume_less_1100,
c.bid_volume_less_1500, c.bid_volume_great_1500,
c.bid_volume_less_500_out, c.bid_volume_less_800_out,
c.bid_volume_less_1500_out, c.bid_volume_great_1500_out,
c.psech_exp_all, c.psech_exp_tranz, c.psech_imp_all,
c.psech_imp_tranz, c.bid_volume_acc, c.bid_volume_10_400,
c.bid_volume_400_600, c.bid_volume_600_700, c.bid_volume_700_800,
c.bid_volume_800_900, c.bid_volume_900_1000,
c.bid_volume_1000_1100, c.bid_volume_1100_1200,
c.bid_volume_1200, c.bid_volume_acc_out,
c.bid_volume_acc_out_ges, c.bid_volume_acc_out_aes_all,
c.bid_volume_acc_out_aes, c.bid_volume_acc_out_aes_rbmk,
c.bid_volume_acc_out_tes_all, c.bid_volume_acc_out_tes,
c.bid_volume_acc_out_tes_bid, c.bid_volume_10_400_out,
c.bid_volume_400_600_out, c.bid_volume_600_700_out,
c.bid_volume_700_800_out, c.bid_volume_800_900_out,
c.bid_volume_900_1000_out, c.bid_volume_1000_1100_out,
c.bid_volume_1100_1200_out, c.bid_volume_1200_out, c.tg_tes,
c.tg_ges, c.tg_aes, c.pmin_agg_tes, c.pmin_tes, c.pmax_tes
from sipr_calculation_result c
where target_date = :tdate
and sipr_calc = :sipr_calc
and scenario = :scenario_code

union all

select 'боевой', c.target_date, c.price_zone,
c.tg_cons, c.pr_cons, c.tg_gen, c.pr_gen, c.bid_volume_less_650,
c.bid_volume_less_800, c.bid_volume_less_1100,
c.bid_volume_less_1500, c.bid_volume_great_1500,
c.bid_volume_less_500_out, c.bid_volume_less_800_out,
c.bid_volume_less_1500_out, c.bid_volume_great_1500_out,
c.psech_exp_all, c.psech_exp_tranz, c.psech_imp_all,
c.psech_imp_tranz, c.bid_volume_acc, c.bid_volume_10_400,
c.bid_volume_400_600, c.bid_volume_600_700, c.bid_volume_700_800,
c.bid_volume_800_900, c.bid_volume_900_1000,
c.bid_volume_1000_1100, c.bid_volume_1100_1200,
c.bid_volume_1200, c.bid_volume_acc_out,
c.bid_volume_acc_out_ges, c.bid_volume_acc_out_aes_all,
c.bid_volume_acc_out_aes, c.bid_volume_acc_out_aes_rbmk,
c.bid_volume_acc_out_tes_all, c.bid_volume_acc_out_tes,
c.bid_volume_acc_out_tes_bid, c.bid_volume_10_400_out,
c.bid_volume_400_600_out, c.bid_volume_600_700_out,
c.bid_volume_700_800_out, c.bid_volume_800_900_out,
c.bid_volume_900_1000_out, c.bid_volume_1000_1100_out,
c.bid_volume_1100_1200_out, c.bid_volume_1200_out, c.tg_tes,
c.tg_ges, c.tg_aes, c.pmin_agg_tes, c.pmin_tes, c.pmax_tes
from tsdb2.sipr_calculation_result c
where target_date = :tdate_init
and sipr_calc = 0
and scenario = 0

order by 3, 1 desc
