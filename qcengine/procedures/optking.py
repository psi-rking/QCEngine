from typing import Any, Dict, Union

from qcelemental.models import Optimization, OptimizationInput
from qcelemental.util import which_import

from .model import ProcedureHarness


class OptkingProcedure(ProcedureHarness):

    _defaults = {"name": "optking", "procedure": "optimization"}

    class Config(ProcedureHarness.Config):
        pass

    def found(self, raise_error: bool=False) -> bool:
        return which_import('optking', return_bool=True, raise_error=raise_error, raise_msg='Please install via `conda install optking -c conda-forge`.')

    def build_input_model(self, data: Union[Dict[str, Any], 'OptimizationInput']) -> 'OptimizationInput':
        return self._build_model(data, OptimizationInput)

    def compute(self, input_data: 'OptimizationInput', config: 'JobConfig') -> 'Optimization':
        try:
            import optking
        except ModuleNotFoundError:
            raise ModuleNotFoundError("Could not find optking in the Python path.")

        optking_input = input_data.dict()

        # Older QCElemental compat, can be removed in v0.6
        if "extras" not in optking_input["input_specification"]:
            optking_input["input_specification"]["extras"] = {}

        # Set retries to two if zero while respecting local_config
        local_config = config.dict()
        local_config["retries"] = local_config.get("retries", 2) or 2
        optking_input["input_specification"]["extras"]["_qcengine_local_config"] = local_config

        # Run the program
        output_data = optking.run_json.optking_run_json(optking_input)

        output_data["provenance"] = {
            "creator": "optking",
            "routine": "optking.run_json.optking_run_json",
            "version": optking.__version__
        }

        output_data["schema_name"] = "qcschema_optimization_output"
        output_data["input_specification"]["extras"].pop("_qcengine_local_config", None)
        if output_data["success"]:
            output_data = Optimization(**output_data)

        return output_data
