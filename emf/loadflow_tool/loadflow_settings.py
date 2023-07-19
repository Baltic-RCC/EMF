import pypowsybl


igm_lf_parameters = {
                      "slackBusSelectionMode": 'MOST_MESHED',
                      'slackBusesIds': '',
                      'lowImpedanceBranchMode': 'REPLACE_BY_ZERO_IMPEDANCE_LINE',
                      'voltageRemoteControl': True,
                      'throwsExceptionInCaseOfSlackDistributionFailure': False,
                      'loadPowerFactorConstant': False,  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
                      'plausibleActivePowerLimit': 5000.0,
                      'slackBusPMaxMismatch': 1.0,
                      'voltagePerReactivePowerControl': False,
                      'reactivePowerRemoteControl': False,
                      'maxNewtonRaphsonIterations': 15,
                      'maxOuterLoopIterations': 20,  # eumd:PowerFlowSettings.maxIterationNumber "20"
                      'newtonRaphsonConvEpsPerEq': 1.0E-4,
                      'voltageInitModeOverride': None,
                      'transformerVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',  # TODO - check this
                      'shuntVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',  # TODO - check this
                      'minPlausibleTargetVoltage': 0.8,
                      'maxPlausibleTargetVoltage': 1.2,
                      'minRealisticVoltage': 0.5,
                      'maxRealisticVoltage': 2.0,
                      'reactiveRangeCheckMode': 'MAX',
                      'lowImpedanceThreshold': 1.0E-5,  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
                      'networkCacheEnabled': False,
                      'svcVoltageMonitoring': True,
                      'stateVectorScalingMode': None,
                      'maxSlackBusCount': 1,  # TODO - check this
                      #'debugDir': '',
                      'incrementalTransformerVoltageControlOuterLoopMaxTapShift': 3,  # TODO - check this
                      'secondaryVoltageControl': False,
                      'controllerToPilotPointVoltageSensiEpsilon': 0.01,
                      'reactiveLimitsMaxPqPvSwitch': 3,
                      'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
                      'maxActivePowerMismatch': 0.1,  # cim:PowerFlowSettings.activePowerTolerance "0.1"
                      'maxReactivePowerMismatch': 0.1,  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
                      'maxVoltageMismatch': 1.0E-4,  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
                      'maxAngleMismatch': 1.0E-5,  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
                      'maxRatioMismatch': 1.0E-5,
                      'maxSusceptanceMismatch': 1.0E-4,
                      'phaseShifterControlMode': 'CONTINUOUS_WITH_DISCRETISATION',
                      'alwaysUpdateNetwork': False,
                      'mostMeshedSlackBusSelectorMaxNominalVoltagePercentile': 95.0,
                      #'reportedFeatures': '',
                      #'slackBusCountryFilter': [],
                      #'actionableSwitchesIds': [],
                      'asymmetrical': False,
                      'minNominalVoltageTargetVoltageCheck': 20.0}

# https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
# https://www.powsybl.org/pages/documentation/simulation/powerflow/
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.interchangeControlEnabled "false" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.respectActivePowerLimits "true" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.staticVarCompensatorControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.switchedShuntControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerPhaseTapControlPriority "1" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerRatioTapControlPriority "1" ;

igm_validation_lf_settings = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=True,  # cim:PowerFlowSettings.transformerRatioTapControlPriority "1" ;
    no_generator_reactive_limits=False,  # cim:PowerFlowSettings.respectReactivePowerLimits "true" ;
    phase_shifter_regulation_on=True,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "1" ;
    twt_split_shunt_admittance=None,
    simul_shunt=True,  # cim:PowerFlowSettings.switchedShuntControlPriority "2" ;
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionParticipationFactor ;
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_PARTICIPATION_FACTOR, #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionParticipationFactor ;
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,  # ALL / MAIN
    provider_parameters=igm_lf_parameters,
)

CGM_DEFAULT_PROVIDER_PARAMETERS = {
    'loadPowerFactorConstant': 'False',  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
    'maxOuterLoopIterations': '30',  # eumd:PowerFlowSettings.maxIterationNumber "20"
    'lowImpedanceThreshold': '1.0E-5',  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',  # cim:PowerFlowSettings.activePowerTolerance "0.1"
    'maxReactivePowerMismatch': '0.1',  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
    'maxVoltageMismatch': '1.0E-4',  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
    'maxAngleMismatch': '1.0E-5',  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
}

# https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
# https://www.powsybl.org/pages/documentation/simulation/powerflow/
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.interchangeControlEnabled "false" ;
# TODO - NOT AVAILABLE - ccim:PowerFlowSettings.respectActivePowerLimits "true" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.staticVarCompensatorControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.switchedShuntControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerPhaseTapControlPriority "1" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerRatioTapControlPriority "1" ;
# TODO - USE IN SCALING - eumd:PowerFlowSettings.maxIterationNumberAIC "15" ;

CGM_DEFAULT = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=True,
    no_generator_reactive_limits=False,  # cim:PowerFlowSettings.respectReactivePowerLimits "true" ;
    phase_shifter_regulation_on=True,
    twt_split_shunt_admittance=None,
    simul_shunt=True,
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly ;
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN, #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly ;
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,  # ALL / MAIN
    provider_parameters=CGM_DEFAULT_PROVIDER_PARAMETERS,
)

cgm_lf_parameters_1 = {
                                      "slackBusSelectionMode": 'MOST_MESHED',
                                      'slackBusesIds': '',
                                      'lowImpedanceBranchMode': 'REPLACE_BY_ZERO_IMPEDANCE_LINE',
                                      'voltageRemoteControl': True,
                                      'throwsExceptionInCaseOfSlackDistributionFailure': False,
                                      'loadPowerFactorConstant': False,  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
                                      'plausibleActivePowerLimit': 5000.0,
                                      'slackBusPMaxMismatch': 1.0,
                                      'voltagePerReactivePowerControl': False,
                                      'reactivePowerRemoteControl': False,
                                      'maxNewtonRaphsonIterations': 15,
                                      'maxOuterLoopIterations': 30,  # eumd:PowerFlowSettings.maxIterationNumber "20"
                                      'newtonRaphsonConvEpsPerEq': 1.0E-4,
                                      'voltageInitModeOverride': None,
                                      'transformerVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',
                                      'shuntVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',
                                      'minPlausibleTargetVoltage': 0.8,
                                      'maxPlausibleTargetVoltage': 1.2,
                                      'minRealisticVoltage': 0.5,
                                      'maxRealisticVoltage': 2.0,
                                      'reactiveRangeCheckMode': 'MAX',
                                      'lowImpedanceThreshold': 1.0E-5,  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
                                      'networkCacheEnabled': False,
                                      'svcVoltageMonitoring': True,
                                      'stateVectorScalingMode': None,
                                      'maxSlackBusCount': 1,
                                      #'debugDir': '',
                                      'incrementalTransformerVoltageControlOuterLoopMaxTapShift': 3,
                                      'secondaryVoltageControl': False,
                                      'controllerToPilotPointVoltageSensiEpsilon': 0.01,
                                      'reactiveLimitsMaxPqPvSwitch': 3,
                                      'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
                                      'maxActivePowerMismatch': 0.1,  # cim:PowerFlowSettings.activePowerTolerance "0.1"
                                      'maxReactivePowerMismatch': 0.1,  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
                                      'maxVoltageMismatch': 1.0E-4,  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
                                      'maxAngleMismatch': 1.0E-5,  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
                                      'maxRatioMismatch': 1.0E-5,
                                      'maxSusceptanceMismatch': 1.0E-4,
                                      'phaseShifterControlMode': 'CONTINUOUS_WITH_DISCRETISATION',
                                      'alwaysUpdateNetwork': False,
                                      'mostMeshedSlackBusSelectorMaxNominalVoltagePercentile': 95.0,
                                      #'reportedFeatures': '',
                                      #'slackBusCountryFilter': [],
                                      #'actionableSwitchesIds': [],
                                      'asymmetrical': False,
                                      'minNominalVoltageTargetVoltageCheck': 20.0}

# https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
# https://www.powsybl.org/pages/documentation/simulation/powerflow/
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.interchangeControlEnabled "false" ;
# TODO - NOT AVAILABLE - ccim:PowerFlowSettings.respectActivePowerLimits "true" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.staticVarCompensatorControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.switchedShuntControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerPhaseTapControlPriority "1" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerRatioTapControlPriority "1" ;
# TODO - USE IN SCALING - eumd:PowerFlowSettings.maxIterationNumberAIC "15" ;

cgm_lf_settings_1 = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=False, #  ; cim:PowerFlowSettings.transformerRatioTapControlPriority "0" ;
    no_generator_reactive_limits=False,  # cim:PowerFlowSettings.respectReactivePowerLimits "true" ;
    phase_shifter_regulation_on=False,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "0":
    twt_split_shunt_admittance=None,
    simul_shunt=False,  # cim:PowerFlowSettings.switchedShuntControlPriority "0" ;
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly ;
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN, #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly ;
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,  # ALL / MAIN
    provider_parameters=cgm_lf_parameters_1,
)

cgm_lf_parameters_2 = {
                                      "slackBusSelectionMode": 'MOST_MESHED',
                                      'slackBusesIds': '',
                                      'lowImpedanceBranchMode': 'REPLACE_BY_ZERO_IMPEDANCE_LINE',
                                      'voltageRemoteControl': True,
                                      'throwsExceptionInCaseOfSlackDistributionFailure': False,
                                      'loadPowerFactorConstant': False,  # cim:PowerFlowSettings.loadVoltageDependency "false" ; TODO - check this
                                      'plausibleActivePowerLimit': 5000.0,
                                      'slackBusPMaxMismatch': 1.0,
                                      'voltagePerReactivePowerControl': False,
                                      'reactivePowerRemoteControl': False,
                                      'maxNewtonRaphsonIterations': 15,
                                      'maxOuterLoopIterations': 30,  # eumd:PowerFlowSettings.maxIterationNumber "20"
                                      'newtonRaphsonConvEpsPerEq': 1.0E-4,
                                      'voltageInitModeOverride': None,
                                      'transformerVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',
                                      'shuntVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',
                                      'minPlausibleTargetVoltage': 0.8,
                                      'maxPlausibleTargetVoltage': 1.2,
                                      'minRealisticVoltage': 0.5,
                                      'maxRealisticVoltage': 2.0,
                                      'reactiveRangeCheckMode': 'MAX',
                                      'lowImpedanceThreshold': 1.0E-5,  # cim:PowerFlowSettings.impedanceThreshold "1e-05" ;
                                      'networkCacheEnabled': False,
                                      'svcVoltageMonitoring': True,
                                      'stateVectorScalingMode': None,
                                      'maxSlackBusCount': 1,
                                      #'debugDir': '',
                                      'incrementalTransformerVoltageControlOuterLoopMaxTapShift': 3,
                                      'secondaryVoltageControl': False,
                                      'controllerToPilotPointVoltageSensiEpsilon': 0.01,
                                      'reactiveLimitsMaxPqPvSwitch': 3,
                                      'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
                                      'maxActivePowerMismatch': 0.5,  # cim:PowerFlowSettings.activePowerTolerance "0.1"
                                      'maxReactivePowerMismatch': 0.5,  # cim:PowerFlowSettings.reactivePowerTolerance "0.1"
                                      'maxVoltageMismatch': 1.0E-4,  # cim:PowerFlowSettings.voltageTolerance "0.0001" ;
                                      'maxAngleMismatch': 1.0E-5,  # cim:PowerFlowSettings.voltageAngleLimit "10" ; TODO - How to convert
                                      'maxRatioMismatch': 1.0E-5,
                                      'maxSusceptanceMismatch': 1.0E-4,
                                      'phaseShifterControlMode': 'CONTINUOUS_WITH_DISCRETISATION',
                                      'alwaysUpdateNetwork': False,
                                      'mostMeshedSlackBusSelectorMaxNominalVoltagePercentile': 95.0,
                                      #'reportedFeatures': '',
                                      #'slackBusCountryFilter': [],
                                      #'actionableSwitchesIds': [],
                                      'asymmetrical': False,
                                      'minNominalVoltageTargetVoltageCheck': 20.0}

# https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
# https://www.powsybl.org/pages/documentation/simulation/powerflow/
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.interchangeControlEnabled "false" ;
# TODO - NOT AVAILABLE - ccim:PowerFlowSettings.respectActivePowerLimits "true" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.staticVarCompensatorControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.switchedShuntControlPriority "2" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerPhaseTapControlPriority "1" ;
# TODO - NOT AVAILABLE - cim:PowerFlowSettings.transformerRatioTapControlPriority "1" ;
# TODO - USE IN SCALING - eumd:PowerFlowSettings.maxIterationNumberAIC "15" ;

cgm_lf_settings_2 = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.UNIFORM_VALUES,  # cim:PowerFlowSettings.flatStart "true"
    transformer_voltage_control_on=False, #  ; cim:PowerFlowSettings.transformerRatioTapControlPriority "0" ;
    no_generator_reactive_limits=True,  # cim:PowerFlowSettings.respectReactivePowerLimits "false" ;
    phase_shifter_regulation_on=False,  # cim:PowerFlowSettings.transformerPhaseTapControlPriority "0":
    twt_split_shunt_admittance=None,
    simul_shunt=False,  # cim:PowerFlowSettings.switchedShuntControlPriority "0" ;
    read_slack_bus=True,
    write_slack_bus=False,
    distributed_slack=True,  #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly ;
    balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN, #cim:PowerFlowSettings.slackDistributionKind cim:SlackDistributionKind.generationDistributionActivePowerAndVoltageNodesOnly ;
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,  # ALL / MAIN
    provider_parameters=cgm_lf_parameters_2
)

# # pypowsybl.loadflow.get_provider_parameters()
# # https://www.powsybl.org/pages/documentation/simulation/powerflow/openlf.html#parameters
# default_parameters = {
#                                       "slackBusSelectionMode": 'MOST_MESHED',
#                                       'slackBusesIds': '',
#                                       'lowImpedanceBranchMode': 'REPLACE_BY_ZERO_IMPEDANCE_LINE',
#                                       'voltageRemoteControl': True,
#                                       'throwsExceptionInCaseOfSlackDistributionFailure': False,
#                                       'loadPowerFactorConstant': False,
#                                       'plausibleActivePowerLimit': 5000.0,
#                                       'slackBusPMaxMismatch': 1.0,
#                                       'voltagePerReactivePowerControl': False,
#                                       'reactivePowerRemoteControl': False,
#                                       'maxNewtonRaphsonIterations': 15,
#                                       'maxOuterLoopIterations': 20,
#                                       'newtonRaphsonConvEpsPerEq': 1.0E-4,
#                                       'voltageInitModeOverride': None,
#                                       'transformerVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',
#                                       'shuntVoltageControlMode': 'WITH_GENERATOR_VOLTAGE_CONTROL',
#                                       'minPlausibleTargetVoltage': 0.8,
#                                       'maxPlausibleTargetVoltage': 1.2,
#                                       'minRealisticVoltage': 0.5,
#                                       'maxRealisticVoltage': 2.0,
#                                       'reactiveRangeCheckMode': 'MAX',
#                                       'lowImpedanceThreshold': 1.0E-8,
#                                       'networkCacheEnabled': False,
#                                       'svcVoltageMonitoring': True,
#                                       'stateVectorScalingMode': None,
#                                       'maxSlackBusCount': 1,
#                                       'debugDir': '',
#                                       'incrementalTransformerVoltageControlOuterLoopMaxTapShift': 3,
#                                       'secondaryVoltageControl': False,
#                                       'controllerToPilotPointVoltageSensiEpsilon': 0.01,
#                                       'reactiveLimitsMaxPqPvSwitch': 3,
#                                       'newtonRaphsonStoppingCriteriaType': 'UNIFORM_CRITERIA',
#                                       'maxActivePowerMismatch': 0.01,
#                                       'maxReactivePowerMismatch': 0.01,
#                                       'maxVoltageMismatch': 1.0E-4,
#                                       'maxAngleMismatch': 1.0E-5,
#                                       'maxRatioMismatch': 1.0E-5,
#                                       'maxSusceptanceMismatch': 1.0E-4,
#                                       'phaseShifterControlMode': 'CONTINUOUS_WITH_DISCRETISATION',
#                                       'alwaysUpdateNetwork': False,
#                                       'mostMeshedSlackBusSelectorMaxNominalVoltagePercentile': 95.0,
#                                       'reportedFeatures': '',
#                                       'slackBusCountryFilter': [],
#                                       'actionableSwitchesIds': [],
#                                       'asymmetrical': False,
#                                       'minNominalVoltageTargetVoltageCheck': 20.0}