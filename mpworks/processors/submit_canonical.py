import json
import os
import warnings
from pymongo import MongoClient
from fireworks.core.launchpad import LaunchPad
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submission.submission_mongo import SubmissionMongoAdapter
from pymatgen import MPRester
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'

def clear_env():
    sma = SubmissionMongoAdapter.auto_load()
    if 'prod' in sma.db:
        warnings.warn("Not clearing production db for safety reasons.")
        return

    lp = LaunchPad.auto_load()

    snl = SNLMongoAdapter.auto_load()

    db_dir = os.environ['DB_LOC']
    db_path = os.path.join(db_dir, 'tasks_db.json')
    with open(db_path) as f:
        db_creds = json.load(f)

    sma._reset()
    lp.reset('', require_password=False)
    snl._reset()

    conn = MongoClient(db_creds['host'], db_creds['port'])
    db = conn[db_creds['database']]
    db.authenticate(db_creds['admin_user'], db_creds['admin_password'])
    db.tasks.remove()
    db.boltztrap.remove()
    db.counter.remove()
    db['dos_fs.chunks'].remove()
    db['dos_fs.files'].remove()
    db['band_structure_fs.files'].remove()
    db['band_structure_fs.files'].remove()


def submit_tests(names=None, params=None, project=None):
    sma = SubmissionMongoAdapter.auto_load()

    # note: TiO2 is duplicated twice purposely, duplicate check should catch this
    compounds = {
	"InSb": 20012, "AgCl": 22922, "InSe": 20485, "CdGeAs2": 4953,
	"CdGeP2": 3668, "BN": 1639, "GaSb": 1156, "RbBr": 22867, "NaCl":
	22862, "GaP": 2490, "GaN": 804, "SnTe": 1883, "AlAs": 2172, "BP":
	1479, "ZnGeAs2": 4008, "KBr": 23251, "PbSe": 2201, "CdSe": 2691,
	"CuGaS2": 5238, "Al2O3": 1143, "LiF": 1138, "NaF": 682, "BeO":
	2542, "NaI": 23268, "LiH": 23703, "AgGaS2": 5342, "CuInTe2": 22261,
	"SiC": 1883, "SrO": 2472, "ZnSiAs2": 3595, "CuGaSe2": 4840, "InN":
	22205, "NaBr": 22916, "ZnTe": 2176, "MgO": 1265, "AlN": 661,
	"CuGaTe2": 3839, "PbTe": 19717, "KCl": 23193, "ZnGeP2": 4524,
	"InP": 20351, "Sb2Te3": 1201, "Bi2Se3": 541837, "Si": 149, "RbCl":
	22867, "CdS": 672, "CdTe": 406, "Cr2O3": 19399, "HgTe": 2730,
	"AlP": 1550, "C_diamond": 66, "GaAs": 2534, "KF": 463, "PbS":
	21276, "IrSb3": 1239, "KI": 22898, "HgSe": 820, "ZnO": 2133,
	"InAs": 20305, "Bi2Te3": 34202, "ZnS": 10695, "Ge": 32, "CaO":
	2605, "CoSb3": 1317, "RbI": 22903, "ZnSe": 1190, "BaO": 1342,
	"AlSb": 2624, "ZnSb": 753
    } if project == 'EosThermal' else {
	"Si": 149, "Al": 134, "ZnO": 2133, "FeO": 18905, "LiCoO2": 601860,
	"LiFePO4": 585433, "GaAs": 2534, "Ge": 32, "PbTe": 19717, "YbO": 1216,
	"SiC": 567551, "Fe3C": 510623, "SiO2": 547211, "Na2O": 2352, "InSb
	(unstable)": 10148, "Sb2O5": 1705, "N2O5": 554368, "BaTiO3": 5020,
	"Rb2O": 1394, "TiO2": 554278, "TiO2 (2)": 554278, 'BaNbTePO8': 560794,
	"AgCl": 22922, "AgCl (2)": 570858, "SiO2 (2)": 555211, "Mg2SiO4": 2895,
	"CO2": 20066, "PbSO4": 22298, "SrTiO3": 5532, "FeAl": 2658, "AlFeCo2":
	10884, "NaCoO2": 554427, "ReO3": 547271, "LaH2": 24153, "SiH3I": 28538,
	"LiBH4": 30209, "H8S5N2": 28143, "LiOH": 23856, "SrO2": 2697, "Mn": 35,
	"Hg4Pt": 2312, "PdF4": 13868, "Gd2WO6": 651333, 'MnO2': 19395, 'VO2': 504800
    }

    mpr = MPRester()

    for name, sid in compounds.iteritems():
        if not names or name in names:
            sid = mpr.get_materials_id_from_task_id("mp-{}".format(sid))
            s = mpr.get_structure_by_material_id(sid, final=False)

            snl = StructureNL(s, 'Patrick Huck <phuck@lbl.gov>')
	    if project is not None:
		snl.projects.append(project)

            parameters = {'priority': 10} if name == 'Si' else {}
            if params:
                parameters.update(params)
            sma.submit_snl(snl, 'phuck@lbl.gov', parameters=parameters)


def clear_and_submit(clear=False, names=None, params=None, project=None):
    if clear:
        clear_env()
    submit_tests(names=names, params=params, project=project)
