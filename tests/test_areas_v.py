import time
from decimal import Decimal
from operator import itemgetter
from sqlalchemy.orm import relationship, subqueryload

from utils import ORM, DB
from utils.progress_bar import update_progress
from eq_db.classes.areas import make_areas, Area
from eq_db.classes.nodes import make_nodes, Node
from eq_db.classes.lines import make_lines, Line
from eq_db.classes.consumers import make_consumers, Consumer
from eq_db.classes.loads import make_loads, Load
from eq_db.classes.dpgs import make_dpgs
from eq_db.classes.dpgs.base_dpg import Dpg
from eq_db.classes.dpgs.dpg_demand import DpgDemand
from eq_db.classes.dpgs.dpg_demand_fsk import DpgDemandFSK
from eq_db.classes.dpgs.dpg_demand_system import DpgDemandSystem
from eq_db.classes.dpgs.dpg_demand_load import DpgDemandLoad
from eq_db.classes.dpgs.dpg_supply import DpgSupply
from eq_db.classes.dpgs.dpg_impex import DpgImpex
from eq_db.classes.stations import make_stations, Station
from eq_db.classes.dgus import make_dgus, Dgu
from eq_db.classes.gus import make_gus, Gu
from eq_db.classes.bids import make_bids, Bid
from eq_db.classes.dgu_groups import make_dgu_groups, DguGroup
from eq_db.classes.sections import make_sections, Section
from eq_db.classes.impex_areas import make_impex_areas, ImpexArea
from eq_db.classes.wsumgen import make_wsumgen, Wsumgen
from eq_db.classes.price_zone import make_price_zones, PriceZone

HOURCOUNT = 24

tsid = 221076901
scenario = 1
tdate = '29-05-2015'

start_time = time.time()

# print('PriceZone %i' % len(ORM.session.query(PriceZone).all()))
# print('Area %i' % len(ORM.session.query(Area).options(ORM.FromCache('default')).all()))
# print('Node %i' % len(ORM.session.query(Node).options(ORM.FromCache('default')).order_by(Node.code).all()))
# print('Line %i' % len(ORM.session.query(Line).options(ORM.FromCache('default')).all()))
# print('Consumer %i' % len(ORM.session.query(Consumer).options(ORM.FromCache('default')).all()))
# print('Load %i' % len(ORM.session.query(Load).options(ORM.FromCache('default')).all()))
# print('DpgSupply %i' % len(ORM.session.query(DpgSupply).options(ORM.FromCache('default')).all()))
# print('DpgDemand %i' % len(ORM.session.query(DpgDemand).options(ORM.FromCache('default')).all()))
# # print('DpgDemandFSK %i' % len(ORM.session.query(DpgDemandFSK).all()))
# # print('DpgDemandSystem %i' % len(ORM.session.query(DpgDemandSystem).all()))
# # print('DpgDemandLoad %i' % len(ORM.session.query(DpgDemandLoad).all()))
# print('DpgImpex %i' % len(ORM.session.query(DpgImpex).options(ORM.FromCache('default')).all()))
# print('Station %i' % len(ORM.session.query(Station).options(ORM.FromCache('default')).all()))
# print('Dgu %i' % len(ORM.session.query(Dgu).options(ORM.FromCache('default')).all()))
# print('Gu %i' % len(ORM.session.query(Gu).options(ORM.FromCache('default')).all()))
# print('Bid %i' % len(ORM.session.query(Bid).options(ORM.FromCache('default')).all()))
# print('DguGroup %i' % len(ORM.session.query(DguGroup).options(ORM.FromCache('default')).all()))
# print('Section %i' % len(ORM.session.query(Section).options(ORM.FromCache('default')).all()))
# print('ImpexArea %i' % len(ORM.session.query(ImpexArea).options(ORM.FromCache('default')).all()))
# print('Wsumgen %i' % len(ORM.session.query(Wsumgen).options(ORM.FromCache('default')).all()))
#
# # fc = []
# # fg = []
# eq = []
# pq = []
# pv = []
# sw = []
# sh = []
# el = []
# cs = []
# crs = []
# s = []
# sl = []
# si = []
# sli = []
# eg = []
# ia = []
# es = []
# ed = []
# pzd = []
# eimp = []
# fd = []
# lh = []
#
# for dgu in Dgu:
#     d = dgu.get_last_hour_data()
#     if d:
#         lh += (d,)
# lh = sorted(lh, key=itemgetter(0))
#
# for price_zone in PriceZone:
#     pzd += (price_zone.get_consumption(),)
# pzd = sorted(pzd, key=itemgetter(0, 1))
#
# for dpg in DpgImpex:
#     eimp += dpg.get_distributed_bid()
# eimp = sorted(eimp, key=itemgetter(1, 0, 3))
#
# for wsum in Wsumgen:
#     fd += (wsum.get_fuel_data(),)
# fd = sorted(fd, key=itemgetter(0, 1))
#
#
# for impex_area in ImpexArea:
#     ia += (impex_area.get_impex_area_data(),)
# # ia = sorted(ia, key=itemgetter(2, 0, 1))
#
# if Node.ky:
#     L = len(Node.lst[Node.ky])
# else:
#     L = len(Node.lst)
#
# for i, node in enumerate(sorted(Node, key=lambda node: node.code)):
#     # fc += node.get_fixedcon_data()
#     # fg += node.get_fixedgen_data()
#     eq += node.get_eq_db_nodes_data()
#     pq += node.get_pq_data()
#     pv += node.get_pv_data()
#     sw += node.get_sw_data()
#     sh += node.get_shunt_data()
#     update_progress((i + 1) / L)
#
# if DguGroup.ky:
#     L = len(DguGroup.lst[DguGroup.ky])
# else:
#     L = len(DguGroup.lst)
#
# for i, dgu_group in enumerate(DguGroup):
#     cs += dgu_group.get_constraint_data()
#     crs += dgu_group.get_dgu_data()
#     update_progress((i + 1) / L)
#
# cs = sorted(cs, key=itemgetter(1, 0))
# crs = sorted(crs, key=itemgetter(1, 0, 2))
#
# if Dgu.ky:
#     L = len(Dgu.lst[Dgu.ky])
# else:
#     L = len(Dgu.lst)
#
# for i, dgu in enumerate(sorted(Dgu, key=lambda dgu: dgu.code)):
#     eg += dgu.get_prepared_generator_data()
#     update_progress((i + 1) / L)
#
# L = len(Dpg.lst)
#
# for i, dpg in enumerate(DpgSupply):
#     es += dpg.get_distributed_bid()
#     update_progress((i + 1) / L)
# es = sorted(es, key=itemgetter(5, 0, 6))
#
# if DpgDemand.ky:
#     L = len(DpgDemand.lst[DpgDemand.ky])
# else:
#     L = len(DpgDemand.lst)
#
# for i, dpg in enumerate(DpgDemand):
#     ed += dpg.get_distributed_bid()
#     update_progress((i + 1) / L)
# ed = sorted(ed, key=itemgetter(1, 3, 0, 2))
#
# if Section.ky:
#     L = len(Section.lst[Section.ky])
# else:
#     L = len(Section.lst)
#
# for i, section in enumerate(Section):
#     s += section.get_section_data()
#     sl += section.get_section_lines_data()
#     si += section.get_section_impex_data()
#     sli += section.get_section_lines_impex_data()
#     update_progress((i + 1) / L)
#
# s = sorted(s, key=itemgetter(1, 0))
# sl = sorted(sl, key=itemgetter(5, 2, 3, 1, 0))
# si = sorted(si, key=itemgetter(1, 0))
# sli = sorted(sli, key=itemgetter(5, 2, 3, 1, 0))
#
# if Line.ky:
#     L = len(Line.lst[Line.ky])
# else:
#     L = len(Line.lst)
#
# for i, line in enumerate(Line):
#     el += line.get_eq_db_lines_data()
#     update_progress((i + 1) / L)
# el = sorted(el, key=itemgetter(1, 2, 3, 0))
#
# print(time.time() - start_time)
#
#
# con = DB.OracleConnection()

# # script = 'SElect hour_num, node_id, u_base, p_cons_minus_gen, q_cons, q_gen, u_max, u_min, cons, gen from tsdb2.wh_eq_db_nodes_pq partition (TS_220482901) order by node_id, hour_num'
# #
# # pq_o = con.exec_script(script)
# #
# # with open('pq_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(pq, pq_o):
# #         tup = tuple(map(lambda x, y: x - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%f;%f;%f;%f;%f;%f;%i;%i;%f;%f;%f;%f;%f;%f;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;%f;%f;%f;%f;%f;%f;' % tup, file=f)
# #
# # script = 'SELECT hour_num, node_id, u_base, p_gen, q_cons, q_gen, type, U, q_max, q_min, U_max, u_min, cons, gen FROM tsdb2.wh_eq_db_nodes_pv PARTITION (ts_220482901) ORDER BY node_id, hour_num'
# #
# # pv_o = con.exec_script(script)
# #
# # with open('pv_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(pv, pv_o):
# #         tup = tuple(map(lambda x, y: x - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%f;%f;%i;%f;%f;%f;%f;%f;%f;%f;%i;%i;%f;%f;%f;%f;%i;%f;%f;%f;%f;%f;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;%f;%f;%i;%f;%f;%f;%f;%f;%f;%f;' % tup, file=f)
# #
# # script = '''SELECT hour_num, node_id, U_base, u_rel, phase_start, P_start, q_max, q_min
# #             FROM tsdb2.wh_eq_db_nodes_sw PARTITION (ts_220482901)
# #             ORDER BY node_id, hour_num'''
# #
# # sw_o = con.exec_script(script)
# #
# # with open('sw_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(sw, sw_o):
# #         tup = tuple(map(lambda x, y: x - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%f;%f;%f;%f;%i;%i;%f;%f;%f;%f;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;%f;%f;%f;%f;' % tup, file=f)
# #
# # script = '''SELECT hour_num, node_id, u_base, start_u, start_phase, u_rated, region_id, price_zone--, pricezonefixed
# #             FROM tsdb2.wh_eq_db_nodes PARTITION (ts_220482901)
# #             ORDER BY node_id, hour_num'''
# #
# # eq_o = con.exec_script(script)
# #
# # with open('eq_db_nodes_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(eq, eq_o):
# #         tup = tuple(map(lambda x, y: x - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%f;%f;%i;%i;%i;%i;%f;%f;%f;%f;%i;%i;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;%f;%f;%i;%i;' % tup, file=f)
#
# # script = '''SELECT hour_num, node_id, U_base, g, b
# #             FROM tsdb2.wh_eq_db_shunts PARTITION (ts_220482901)
# #             ORDER BY node_id, hour_num'''
# #
# # sh_o = con.exec_script(script)
# #
# # with open('shunts_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(sh, sh_o):
# #         tup = tuple(map(lambda x, y: x - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%f;%i;%i;%f;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;%f;' % tup, file=f)
#
# # script = '''SELECT hour_num, node_from_id, node_tO_id, parallel_num, type,
# #             u_base, base_coef, r,x,g,b,ktr, lagging, b_begin, b_end
# #             FROM tsdb2.wh_eq_db_lines PARTITION (ts_220482901)
# #             ORDER BY node_from_id, node_tO_id, parallel_num, hour_num'''
# #
# # el_o = con.exec_script(script)
# #
# # with open('eq_db_lines_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(el, el_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%i;%i;%i;%f;%f;%f;%f;%f;%f;%f;%f;%f;%f;%i;%i;%i;%i;%i;%f;%f;%f;%f;%f;%f;%f;%f;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%i;%i;%i;%f;%f;%f;%f;%f;%f;%f;%f;%f;%f;' % tup, file=f)
# #
# # script = '''select hour_num, group_id, p_max, p_min
# #             from tsdb2.wh_eq_db_group_constraints partition (TS_220482901)
# #             where group_id <> 0
# #             order by group_id, hour_num'''
# #
# # cs_o = con.exec_script(script)
# #
# # with open('eq_db_constraints_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(cs, cs_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%i;%i;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;' % tup, file=f)
# #
# #
# # script = '''select hour_num, group_id, gen_id
# #             from tsdb2.wh_eq_db_group_constraint_rges partition (TS_220482901)
# #             where group_id <> 0
# #             order by group_id, hour_num, gen_id'''
# #
# # crs_o = con.exec_script(script)
# #
# # with open('eq_db_constraint_rge_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(crs, crs_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%i;%i;%i;%i;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%i;' % tup, file=f)
#
# # script = '''select hour_num, section_id, p_max_forward, p_max_backward
# #             from tsdb2.wh_eq_db_sections partition (TS_220482901)
# #             where is_impex = 0
# #             order by section_id, hour_num'''
# #
# # s_o = con.exec_script(script)
# #
# # with open('eq_db_sections_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(s, s_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%f;%i;%i;%f;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;%f;' % tup, file=f)
# #
# #
# # script = '''select sl.hour_num, sl.parallel_num, node_from_id, node_to_id,
# #             div_coef, sl.section_id
# #             from tsdb2.wh_eq_db_section_lines partition (TS_220482901) sl,
# #             tsdb2.wh_eq_db_sections partition (TS_220482901) s
# #             where s.is_impex = 0
# #             and s.hour_num = sl.hour_num
# #             and s.section_id = sl.section_id
# #             order by sl.section_id, node_from_id, node_to_id, sl.parallel_num, sl.hour_num'''
# #
# # sl_o = con.exec_script(script)
# #
# # with open('eq_db_section_lines_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(sl, sl_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%i;%i;%f;%i;%i;%i;%i;%i;%f;%i;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%i;%i;%f;%i;' % tup, file=f)
# #
# # script = '''select hour_num, section_id, 0
# #             from tsdb2.wh_eq_db_sections partition (TS_220482901)
# #             where is_impex = 1
# #             order by section_id, hour_num'''
# #
# # si_o = con.exec_script(script)
# #
# # with open('eq_db_sections_impex_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(si, si_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%f;%i;%i;%f;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%f;' % tup, file=f)
# #
# #
# # script = '''select sl.hour_num, sl.parallel_num, node_from_id, node_to_id,
# #             div_coef, sl.section_id
# #             from tsdb2.wh_eq_db_section_lines partition (TS_220482901) sl,
# #             tsdb2.wh_eq_db_sections partition (TS_220482901) s
# #             where s.is_impex = 1
# #             and s.hour_num = sl.hour_num
# #             and s.section_id = sl.section_id
# #             order by sl.section_id, node_from_id, node_to_id, sl.parallel_num, sl.hour_num'''
# #
# # sli_o = con.exec_script(script)
# #
# # with open('eq_db_section_lines_impex_data.csv', 'w', encoding='utf-8') as f:
# #     for n, o in zip(sli, sli_o):
# #         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
# #         if sum(map(abs, tup)) > 1e-3:
# #             print(tup + n)
# #             print('%i;%i;%i;%i;%f;%i;%i;%i;%i;%i;%f;%i;' % (tup + n), file=f)
# #         else:
# #             print('%i;%i;%i;%i;%f;%i;' % tup, file=f)
#
# script = '''select hour_num, gen_id, p_min, p, ramp_up, ramp_down
#             from tsdb2.wh_eq_db_generators partition (TS_220482901)
#             order by gen_id, hour_num'''
#
# eg_o = con.exec_script(script)
#
# with open('eq_db_generators_data.csv', 'w', encoding='utf-8') as f:
#     for n, o in zip(eg, eg_o):
#         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
#         if sum(map(abs, tup)) > 1e-3:
#             print(tup + n)
#             print('%i;%i;%f;%f;%f;%f;%i;%i;%f;%f;%f;%f;' % (tup + n), file=f)
#         else:
#             print('%i;%i;%f;%f;%f;%f;' % tup, file=f)

# script = '''select hour_num, node_id, p_max, p_min, cost, gen_id, interval_num, integral_constr_id, tariff, forced_sm
#             from tsdb2.wh_eq_Db_supplies partition (TS_220482901)
#             where p_max > 1e-10
#             order by gen_id, hour_num, interval_num'''
#
# es_o = con.exec_script(script)
#
# with open('eq_db_supplies_data.csv', 'w', encoding='utf-8') as f:
#     for n, o in zip(es, es_o):
#         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
#         if sum(map(abs, tup)) > 1e-3:
#             print(tup + n)
#             print('%i;%i;%f;%f;%f;%i;%i;%i;%f;%i;%i;%i;%f;%f;%f;%i;%i;%i;%f;%i;' % (tup + n), file=f)
#         else:
#             print('%i;%i;%f;%f;%f;%i;%i;%i;%f;%i;' % tup, file=f)

# script = '''select hour_num, id, interval_num, node_id, volume, price, nvl(is_accepted,0)
#             from tsdb2.wh_eq_Db_demands partition (TS_220482901)
#             order by id, node_id, hour_num, interval_num'''
#
# ed_o = con.exec_script(script)
#
# with open('eq_db_demands_data.csv', 'w', encoding='utf-8') as f:
#     for n, o in zip(ed, ed_o):
#         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
#         if sum(map(abs, tup)) > 1e-3:
#             print(tup + n)
#             print('%i;%i;%i;%i;%f;%f;%i;%i;%i;%i;%i;%f;%f;%i;' % (tup + n), file=f)
#         else:
#             print('%i;%i;%i;%i;%f;%f;%i;' % tup, file=f)

# script = '''SELECT hour_num, section_number, direction, interval_num, volume, price, is_accepting
#           FROM tsdb2.wh_eq_db_impexbids partition (TS_221076901) a
#           order by section_number, hour_num, interval_num'''
#
# eimp_o = con.exec_script(script)
#
# with open('eq_db_impexbids_data.csv', 'w', encoding='utf-8') as f:
#     for n, o in zip(eimp, eimp_o):
#         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
#         if sum(map(abs, tup)) > 1e-3:
#             print(tup + n)
#             print('%i;%i;%i;%i;%f;%f;%i;%i;%i;%i;%i;%f;%f;%i;' % (tup + n), file=f)
#         else:
#             print('%i;%i;%i;%i;%f;%f;%i;' % tup, file=f)
#
# script = '''SELECT id, gen_id, p_min, p_max, hour_start, hour_end
#             FROM tsdb2.wh_eq_db_fuel partition (TS_221076901) a
#             order by id, gen_id'''
#
# fd_o = con.exec_script(script)
#
# with open('eq_db_fuel_data.csv', 'w', encoding='utf-8') as f:
#     for n, o in zip(fd, fd_o):
#         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
#         if sum(map(abs, tup)) > 1e-3:
#             print(tup + n)
#             print('%i;%i;%f;%f;%i;%i;%i;%i;%f;%f;%i;%i;' % (tup + n), file=f)
#         else:
#             print('%i;%i;%f;%f;%i;%i;' % tup, file=f)

# script = '''SELECT a.hour_num, a.price_zone, a.p_cons_sum_value
#             FROM tsdb2.wh_eq_db_price_zone_sum_values partition (TS_221076901) a
#             order by hour_num, price_zone'''
#
# pzd_o = con.exec_script(script)
#
# with open('eq_db_price_zone_sum_values_data.csv', 'w', encoding='utf-8') as f:
#     for n, o in zip(pzd, pzd_o):
#         tup = tuple(map(lambda x, y: Decimal(x) - Decimal(y), n, o))
#         if sum(map(abs, tup)) > 1e-3:
#             print(tup + n)
#             print('%i;%i;%f;%i;%i;%f;' % (tup + n), file=f)
#         else:
#             print('%i;%i;%f;' % tup, file=f)

# ORM.recreate_all()
#
sections = make_sections(tsid, tdate)

dgu_groups = make_dgu_groups(tsid, tdate)

bids = make_bids(tsid, tdate)

stations = make_stations(tsid, tdate)

areas = make_areas(tsid, tdate)

impex_areas = make_impex_areas(tsid, tdate)

nodes = make_nodes(tsid, tdate)

loads = make_loads(tsid, tdate)

consumers = make_consumers(tsid, tdate)

dpgs = make_dpgs(tsid, tdate)

dgus = make_dgus(tsid, tdate)

wsumgen = make_wsumgen(tsid, tdate)

gus = make_gus(tsid, tdate)

lines = make_lines(tsid, tdate)

price_zones = make_price_zones()
