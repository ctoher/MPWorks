import socket
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from custodian.custodian import Custodian
from custodian.vasp.handlers import VaspErrorHandler, UnconvergedErrorHandler, PoscarErrorHandler
from custodian.vasp.jobs import VaspJob
import shlex

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class CustodianTask(FireTaskBase, FWSerializable):

    _fw_name = "Custodian Task"

    def run_task(self, fw_spec):
        if 'cvrsvc' in socket.gethostname():  # carver
            v_exe = shlex.split('mpirun -n 8 vasp')  # TODO: make ncores dynamic!
        elif 'nid' in socket.gethostname():  # hopper
            v_exe = shlex.split('aprun -n 24 vasp')  # TODO: make ncores dynamic!
        else:
            raise ValueError('Unrecognized host!')

        handlers = [VaspErrorHandler(), UnconvergedErrorHandler(), PoscarErrorHandler()]
        job = VaspJob.double_relaxation_run(v_exe)
        job.gzipped = False

        c = Custodian(handlers, job, max_errors=10)
        c.run()

        # return FWAction('CONTINUE', {}, {'$set': {'prev_VASP_dir': os.getcwd()}})