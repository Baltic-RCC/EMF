import pypowsybl

"""
Related documentation:
https://www.powsybl.org/pages/documentation/simulation/powerflow/openlf.html#parameters
https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
https://www.powsybl.org/pages/documentation/simulation/powerflow/
"""

# TODO - NOT AVAILABLE - cim:PowerFlowSettings.interchangeControlEnabled "false" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.respectActivePowerLimits "true" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.staticVarCompensatorControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.switchedShuntControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerPhaseTapControlPriority "1" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerRatioTapControlPriority "1" ;
# TODO - USE IN SCALING - eumd:PowerFlowSettings.maxIterationNumberAIC "15" ;

# DEFAULT settings applicable for all processes
OPENLOADFLOW_DEFAULT_PROVIDER = {
    'slackBusesIds': '',
    'lowImpedanceBranchMode': 'REPLACE_BY_ZERO_IMPEDANCE_LINE', 
    'voltageRemoteControl': 'True',
    'loadPowerFactorConstant': 'True',  # cim:PowerFlowSettings.loadVoltageDependency TODO - check this
    'plausibleActivePowerLimit': '1900.0',
    'slackBusPMaxMismatch': '0.1', #slackBusDistributionThreshold
    'voltagePerReactivePowerControl': 'True',
    'newtonRaphsonConvEpsPerEq': '1.0E-4',
    'voltageTargetPriorities': 'GENERATOR,TRANSFORMER,SHUNT',
    'transformerVoltageControlMode': 'AFTER_GENERATOR_VOLTAGE_CONTROL',  
    'shuntVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',  
    'minPlausibleTargetVoltage': '0.8',
    'maxPlausibleTargetVoltage': '1.2',
    'minRealisticVoltage': '0.4',
    'maxRealisticVoltage': '2.0',
    'reactiveRangeCheckMode': 'MAX',
    'networkCacheEnabled': 'False',
    'svcVoltageMonitoring': 'False',
    'maxSlackBusCount': '1',  # TODO - check this
    # 'debugDir': '',
    'incrementalTransformerVoltageControlOuterLoopMaxTapShift': '3',  # TODO - check this
    'secondaryVoltageControl': 'False',
    'reactiveLimitsMaxPqPvSwitch': '3',
    'newtonRaphsonStoppingCriteriaType': 'UNIFORM_CRITERIA',
    'maxActivePowerMismatch': '0.01',  # cim:PowerFlowSettings.activePowerTolerance
    'maxReactivePowerMismatch': '0.01',  # cim:PowerFlowSettings.reactivePowerTolerance
    'maxVoltageMismatch': '1.0E-4',  # cim:PowerFlowSettings.voltageTolerance
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit TODO - How to convert
    'maxRatioMismatch': '1.0E-5',
    'maxSusceptanceMismatch': '1.0E-4',
    'phaseShifterControlMode': 'INCREMENTAL',
    'alwaysUpdateNetwork': 'False',
    'mostMeshedSlackBusSelectorMaxNominalVoltagePercentile': '95.0',
    # 'reportedFeatures': [],
    # 'slackBusCountryFilter': [],
    # 'actionableSwitchesIds': [],
    'asymmetrical': 'False',
    'minNominalVoltageTargetVoltageCheck': '20.0',
    # For loadflow
    'stateVectorScalingMode': 'MAX_VOLTAGE_CHANGE',
    'voltageInitModeOverride': 'FULL_VOLTAGE',
    # Fix Kirchoff 1st law error
    'slackDistributionFailureBehavior': 'FAIL',
    #new
    'dcPowerFactor': '1.0',
    'transformerReactivePowerControl': 'True',
    'useLoadModel': 'False',
    'useActiveLimits': 'True',
    'lineSearchStateVectorScalingMaxIteration': '10',
    'lineSearchStateVectorScalingStepFold': '1.33',
    'maxVoltageChangeStateVectorScalingMaxDv': '0.1',
    'maxVoltageChangeStateVectorScalingMaxDphi': '0.1745',

}

OPENLOADFLOW_DEFAULT = pypowsybl.loadflow.Parameters(
    #voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart
    transformer_voltage_control_on=False,  # cim:PowerFlowSettings.transformerRatioTapControlPriority
    use_reactive_limits=True,  # cim:PowerFlowSettings.respectReactivePowerLimits
    phase_shifter_regulation_on=False,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority
    twt_split_shunt_admittance=False,
    shunt_compensator_voltage_control_on=False,  # cim:PowerFlowSettings.switchedShuntControlPriority
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  # cim:PowerFlowSettings.slackDistributionKind
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN,  # cim:PowerFlowSettings.slackDistributionKind
    dc_use_transformer_ratio=True,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,  # ALL / MAIN - defines islands to be solved
    provider_parameters=OPENLOADFLOW_DEFAULT_PROVIDER,
)


# Deviation of default provider from the default
## Used for CGM main merging  process
__IGM_VALIDATION_PROVIDER = {
    'slackBusSelectionMode': 'MOST_MESHED',
    'referenceBusSelectionMode':'GENERATOR_REFERENCE_PRIORITY',
    'generatorReactivePowerRemoteControl': 'True',
    'reactivePowerRemoteControl': 'True',
    'maxNewtonRaphsonIterations': '30',
    'maxOuterLoopIterations': '30',  
    'loadPowerFactorConstant': 'False',  # cim:PowerFlowSettings.loadVoltageDependency "false"
    'lowImpedanceThreshold': '0.00003',  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '0.0001',  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10"
    'transformerVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',
    'voltagePerReactivePowerControl': 'True',
    'disableVoltageControlOfGeneratorsOutsideActivePowerLimits': 'true', # supress q part of igm-ssh-vs-cgm-ssh error
}
__EU_DEFAULT_PROVIDER = {
    'slackBusSelectionMode': 'LARGEST_GENERATOR',
    'referenceBusSelectionMode':'GENERATOR_REFERENCE_PRIORITY',
    'generatorReactivePowerRemoteControl': 'True',
    'reactivePowerRemoteControl': 'True',
    'maxNewtonRaphsonIterations': '50',
    'maxOuterLoopIterations': '50',  
    'loadPowerFactorConstant': 'True',  # cim:PowerFlowSettings.loadVoltageDependency "false" TODO - check this
    'lowImpedanceThreshold': '0.00003',  # cim:PowerFlowSettings.impedanceThreshold "1e-05"
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '0.0001',  # cim:PowerFlowSettings.voltageTolerance "0.0001"
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10" TODO - How to convert
    'slackBusPMaxMismatch': '0.1',  # To fulfill QOCDC SV_INJECTION_LIMIT = 0.1'
    'disableVoltageControlOfGeneratorsOutsideActivePowerLimits': 'true', # supress q part of igm-ssh-vs-cgm-ssh error
    'disableInconsistentVoltageControls': 'true',
    'transformerVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',
    'shuntVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',
    'phaseShifterControlMode': 'INCREMENTAL',
    

}
__EU_RELAXED_PROVIDER = {
    'slackBusSelectionMode': 'LARGEST_GENERATOR',
    'referenceBusSelectionMode':'GENERATOR_REFERENCE_PRIORITY',
    'generatorReactivePowerRemoteControl': 'True',
    'reactivePowerRemoteControl': 'True',
    'maxNewtonRaphsonIterations': '50',
    'maxOuterLoopIterations': '50',  
    'loadPowerFactorConstant': 'True',  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
    'lowImpedanceThreshold': '0.00003',  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '0.0001',  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
    'slackBusPMaxMismatch': '0.09',  # To fulfill QOCDC SV_INJECTION_LIMIT = 0.1
    'disableVoltageControlOfGeneratorsOutsideActivePowerLimits': 'true', # supress q part of igm-ssh-vs-cgm-ssh error
    'disableInconsistentVoltageControls': 'true',
    'transformerVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',
    'shuntVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',
    'phaseShifterControlMode': 'INCREMENTAL',
}

## Baltic merge parameters
__BA_DEFAULT_PROVIDER = {
    'slackBusSelectionMode': 'MOST_MESHED',
    'generatorReactivePowerRemoteControl': 'True',
    'maxOuterLoopIterations': '30',  # eumd:PowerFlowSettings.maxIterationNumber "30"
    'maxNewtonRaphsonIterations': '15',
    'loadPowerFactorConstant': 'False',  # cim:PowerFlowSettings.loadVoltageDependency "false" TODO - check this
    'lowImpedanceThreshold': '1.0E-5',  # cim:PowerFlowSettings.impedanceThreshold "1e-05"
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '1.0E-4',  # cim:PowerFlowSettings.voltageTolerance "0.0001"
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10" TODO - How to convert
    'slackBusPMaxMismatch': '0.09',  # To fulfill QOCDC SV_INJECTION_LIMIT = 0.1
}
__BA_RELAXED_1_PROVIDER = {
    'slackBusSelectionMode': 'MOST_MESHED',
    'generatorReactivePowerRemoteControl': 'False',
    'maxOuterLoopIterations': '30',  # eumd:PowerFlowSettings.maxIterationNumber "30"
    'maxNewtonRaphsonIterations': '15',
    'loadPowerFactorConstant': 'False',  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
    'lowImpedanceThreshold': '1.0E-5',  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '1.0E-4',  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
    'slackBusPMaxMismatch': '0.09',  # To fulfill QOCDC SV_INJECTION_LIMIT = 0.1
}

__BA_RELAXED_2_PROVIDER = {
    'slackBusSelectionMode': 'MOST_MESHED',
    'maxOuterLoopIterations': '30',  # eumd:PowerFlowSettings.maxIterationNumber "30"
    'maxNewtonRaphsonIterations': '15',
    # 'loadPowerFactorConstant': 'False',  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
    'loadPowerFactorConstant': 'True',
    'lowImpedanceThreshold': '1.0E-5',  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '1.0E-4',  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
    'slackBusPMaxMismatch': '0.09',  # To fulfill QOCDC SV_INJECTION_LIMIT = 0.1
    'disableVoltageControlOfGeneratorsOutsideActivePowerLimits': 'true', # supress q part of igm-ssh-vs-cgm-ssh error
}

# Preparing PROVIDER settings options from default settings
## Used for CGM main merging process
IGM_VALIDATION_PROVIDER = OPENLOADFLOW_DEFAULT_PROVIDER.copy()
IGM_VALIDATION_PROVIDER.update(__IGM_VALIDATION_PROVIDER)
EU_DEFAULT_PROVIDER = OPENLOADFLOW_DEFAULT_PROVIDER.copy()
EU_DEFAULT_PROVIDER.update(__EU_DEFAULT_PROVIDER)
EU_RELAXED_PROVIDER = OPENLOADFLOW_DEFAULT_PROVIDER.copy()
EU_RELAXED_PROVIDER.update(__EU_RELAXED_PROVIDER)

## Baltic merge parameters
BA_DEFAULT_PROVIDER = OPENLOADFLOW_DEFAULT_PROVIDER.copy()
BA_DEFAULT_PROVIDER.update(__BA_DEFAULT_PROVIDER)
BA_RELAXED_1_PROVIDER = OPENLOADFLOW_DEFAULT_PROVIDER.copy()
BA_RELAXED_1_PROVIDER.update(__BA_RELAXED_1_PROVIDER)
BA_RELAXED_2_PROVIDER = OPENLOADFLOW_DEFAULT_PROVIDER.copy()
BA_RELAXED_2_PROVIDER.update(__BA_RELAXED_2_PROVIDER)

# Prepare pypowsybl loadflow parameters classes
## Used for CGM main merging  process
IGM_VALIDATION = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=True,  # cim:PowerFlowSettings.transformerRatioTapControlPriority "1"
    use_reactive_limits=True,  # cim:PowerFlowSettings.respectReactivePowerLimits "true"
    phase_shifter_regulation_on=False,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "1"
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=True,  # cim:PowerFlowSettings.switchedShuntControlPriority "2"
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionParticipationFactor
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_PARTICIPATION_FACTOR,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionParticipationFactor
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,
    provider_parameters=IGM_VALIDATION_PROVIDER,
)

EU_DEFAULT = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=True,  # @cim:PowerFlowSettings.transformerRatioTapControlPriority": "1"
    use_reactive_limits=True,  # cim:PowerFlowSettings.respectReactivePowerLimits "true"
    phase_shifter_regulation_on=True,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "1"
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=True,  # cim:PowerFlowSettings.switchedShuntControlPriority "2"
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN, #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    dc_use_transformer_ratio=True,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.ALL,
    provider_parameters=EU_DEFAULT_PROVIDER,
    dc_power_factor=1.0,
)

EU_RELAXED = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=True,  # cim:PowerFlowSettings.transformerRatioTapControlPriority "0"
    use_reactive_limits=False,  # cim:PowerFlowSettings.respectReactivePowerLimits "false"
    phase_shifter_regulation_on=True,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "1"
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=True,  # cim:PowerFlowSettings.switchedShuntControlPriority "2"
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN, #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    dc_use_transformer_ratio=True,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.ALL,
    provider_parameters=EU_DEFAULT_PROVIDER,
    dc_power_factor=1.0,
)

## Baltic merge parameters
BA_DEFAULT = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=True,  # @cim:PowerFlowSettings.transformerRatioTapControlPriority": "1"
    use_reactive_limits=True,  # cim:PowerFlowSettings.respectReactivePowerLimits "true"
    phase_shifter_regulation_on=True,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "1"
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=True,  # cim:PowerFlowSettings.switchedShuntControlPriority "2"
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.ALL,
    provider_parameters=BA_DEFAULT_PROVIDER,
)

BA_RELAXED_1 = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=False,  # cim:PowerFlowSettings.transformerRatioTapControlPriority "0"
    use_reactive_limits=True,  # cim:PowerFlowSettings.respectReactivePowerLimits "true"
    phase_shifter_regulation_on=False,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "0"
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=False,  # cim:PowerFlowSettings.switchedShuntControlPriority "0"
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.ALL,
    provider_parameters=BA_RELAXED_1_PROVIDER,
)

BA_RELAXED_2 = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=False,  # cim:PowerFlowSettings.transformerRatioTapControlPriority "0"
    use_reactive_limits=False,  # cim:PowerFlowSettings.respectReactivePowerLimits "false"
    phase_shifter_regulation_on=False,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "0"
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=False,  # cim:PowerFlowSettings.switchedShuntControlPriority "0"
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  # cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_CONFORM_LOAD,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.ALL,
    provider_parameters=BA_RELAXED_2_PROVIDER,
)
