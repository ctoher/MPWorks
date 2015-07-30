from monty.os.path import zpath

__author__ = 'Cormac Toher'


from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_input import Incar, Poscar
from pymatgen.agl_thermal.agl_vasp_setup import ModifyVolume
from fireworks.core.firework import Firework, Workflow
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspToDBTask
from mpworks.firetasks.custodian_task import get_custodian_task
from fireworks.utilities.fw_utilities import get_slug
from pymatgen import Composition
from pymatgen.matproj.snl import StructureNL
from mpworks.workflows import snl_to_wf
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL

def update_spec_force_convergence(spec):
    fw_spec = spec
    update_set = {"ENCUT": 600, "EDIFF": 0.00005}
    fw_spec['vasp']['incar'].update(update_set)
    kpoints = spec['vasp']['kpoints']
    k = [2*k for k in kpoints['kpoints'][0]]
    fw_spec['vasp']['kpoints']['kpoints'] = [k]
    return fw_spec


class SetupFConvergenceTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Force Convergence Task"

    def run_task(self, fw_spec):
        incar = fw_spec['vasp']['incar']
        update_set = {"ENCUT": 600, "EDIFF": 0.00005}
        incar.update(update_set)
        #if fw_spec['double_kmesh']:
        kpoints = fw_spec['vasp']['kpoints']
        k = [2*k for k in kpoints['kpoints'][0]]
        kpoints['kpoints'] = [k]
        return FWAction()

class SetupAGLThermalTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup AGL Thermal Task"

    def run_task(self, fw_spec):
        incar = Incar.from_file(zpath("INCAR"))
        incar.update({"NSW": 0})
        incar.write_file("INCAR")
        return FWAction()

class SetupModifiedVolumeStructTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Modified Volume Struct Task"

    def run_task(self, fw_spec):
#        pass
#        """
        relaxed_struct = Structure.from_dict(fw_spec['output']['crystal'])
        modified_structs = ModifyVolume(relaxed_struct)
        fws=[]
        connections={}
        wf=[]

        for i, strainfactor in enumerate(modified_structs.keys()):
            mod_struct = modified_structs[strainfactor]
            f = Composition(mod_struct.formula).alphabetical_formula
            snl = StructureNL(mod_struct, 'Cormac Toher <cormac.toher@duke.edu>')

            tasks = [AddSNLTask()]
            snl_priority = fw_spec.get('priority', 1)
            spec = {'task_type': 'Add Modified Volume Struct to SNL database', 'snl': snl.as_dict(), '_queueadapter': QA_DB, '_priority': snl_priority}
            if 'snlgroup_id' in fw_spec and isinstance(snl, MPStructureNL):
                spec['force_mpsnl'] = snl.as_dict()
                spec['force_snlgroup_id'] = fw_spec['snlgroup_id']
                del spec['snl']
            fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-1000+i*10))
            connections[-1000+i*10] = [-999+i*10]

            spec = snl_to_wf._snl_to_spec(snl)
            spec = update_spec_force_convergence(spec)
            spec['run_tags'].append((strainfactor,))
            #Turn off dupefinder for deformed structure
            del spec['_dupefinder']

            spec['task_type'] = "Calculate deformed structure"
            fws.append(Firework([VaspWriterTask(), SetupAGLThermalTask(),
                                 get_custodian_task(spec)], spec, name=get_slug(f + '--' + fw_spec['task_type']), fw_id=-999+i*10))

            priority = fw_spec['_priority']
            spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
            fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=-998+i*10))
            connections[-999+i*10] = [-998+i*10]

            wf.append(Workflow(fws, connections))
        return FWAction(additions=wf)
#        """

