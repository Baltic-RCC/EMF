"""
Synthetic CGM for post-processing tests: two IGMs (A, B) meeting at boundary nodes, plus the
merged SV and SSH as they come out of pypowsybl + create_updated_ssh.

Every post-processing branch has a trigger in here (the comment says which function uses it):

  XN1  boundary node shared by A and B, paired injections EI_A1/EI_B1, duplicate SvVoltage in
       merged SV (one zero, one 401 kV)                        -> remove_duplicate_sv_voltages,
                                                                  set_paired_boundary_injections_to_zero
  XN2  shared boundary node, merged voltage 0/0 but injection
       non-zero, T_EI_A2 disconnected with flow                 -> check_energized_boundary_nodes,
                                                                  check_for_disconnected_terminals (zeroing)
  XN3  boundary node of A only, energized, T_EI_A3 disconnected
       with flow                                                -> check_for_disconnected_terminals (connect)
  EI_INT non-boundary equivalent injection with SV/SSH mismatch -> check_non_boundary_equivalent_injections
  ES1, ENI1 EnergySource / ExternalNetworkInjection mismatches -> check_all_kind_of_injections
  SM1..SM4 rotating machines (eligible / no control / P outside
       unit limits / inside curve limits)                       -> check_non_regulating_rotating_machine_q,
                                                                  check_rotating_machine_q_outside_p_limits
  TC1..TC3 tap changers (LTC eligible / not LTC / no SvTapStep) -> check_non_ltc_tap_changer_step,
                                                                  add_missing_sv_tap_steps
  CA1  control area with a tie flow, netInterchange 100 vs 95   -> check_net_interchanges
  ESH1 EquivalentShunt with SvShuntCompensatorSections          -> remove_equivalent_shunt_section
  ISL_SMALL topological island of 2 nodes                        -> remove_small_islands
  FM_* FullModel headers, SV missing TP/SSH dependencies        -> check_and_fix_dependencies
"""
from helpers import triplets


def _rows_original():
    rows = []
    add = rows.append
    # headers
    for fm, inst, profile in [("FM_A_TP", "A_TP", "http://entsoe.eu/CIM/Topology/4/1"),
                              ("FM_B_TP", "B_TP", "http://entsoe.eu/CIM/Topology/4/1"),
                              ("FM_A_EQ", "A_EQ", "http://entsoe.eu/CIM/EquipmentCore/3/1")]:
        rows += [(fm, "Type", "FullModel", inst), (fm, "Model.profile", profile, inst)]
    # topological nodes
    for tn, boundary in [("XN1", "true"), ("XN2", "true"), ("XN3", "true"), ("TN_A", "false"), ("TN_B", "false")]:
        rows += [(tn, "Type", "TopologicalNode", "BD"), (tn, "TopologicalNode.boundaryPoint", boundary, "BD")]
    # equivalent injections with terminals
    for ei, tn, connected, p, q, inst in [("EI_A1", "XN1", "true", "10", "1", "A"),
                                          ("EI_B1", "XN1", "true", "-10", "-1", "B"),
                                          ("EI_A2", "XN2", "false", "5", "0.5", "A"),
                                          ("EI_B2", "XN2", "true", "-5", "-0.5", "B"),
                                          ("EI_A3", "XN3", "false", "4", "0.4", "A"),
                                          ("EI_INT", "TN_A", "true", "10", "2", "A")]:
        t = f"T_{ei}"
        rows += [(ei, "Type", "EquivalentInjection", inst), (ei, "EquivalentInjection.p", p, inst),
                 (ei, "EquivalentInjection.q", q, inst), (ei, "EquivalentInjection.regulationStatus", "true", inst),
                 (t, "Type", "Terminal", inst), (t, "Terminal.ConductingEquipment", ei, inst),
                 (t, "Terminal.TopologicalNode", tn, inst), (t, "ACDCTerminal.connected", connected, inst)]
    # original SV: each IGM reports its own boundary voltage -> XN1/XN2 are shared
    for sv, tn, inst in [("SVV_A_XN1", "XN1", "A"), ("SVV_B_XN1", "XN1", "B"),
                         ("SVV_A_XN2", "XN2", "A"), ("SVV_B_XN2", "XN2", "B"), ("SVV_A_XN3", "XN3", "A")]:
        rows += [(sv, "Type", "SvVoltage", inst), (sv, "SvVoltage.TopologicalNode", tn, inst),
                 (sv, "SvVoltage.v", "400", inst), (sv, "SvVoltage.angle", "1", inst)]
    for t, p in [("T_EI_A1", "10"), ("T_EI_B1", "-10"), ("T_EI_A2", "5"), ("T_EI_B2", "-5"), ("T_EI_A3", "4")]:
        f = f"SPF_O_{t}"
        rows += [(f, "Type", "SvPowerFlow", "A"), (f, "SvPowerFlow.Terminal", t, "A"),
                 (f, "SvPowerFlow.p", p, "A"), (f, "SvPowerFlow.q", "0", "A")]
    # rotating machines
    rows += [("RC1", "Type", "RegulatingControl", "A"), ("RC1", "RegulatingControl.enabled", "true", "A"),
             ("RC2", "Type", "RegulatingControl", "A"), ("RC2", "RegulatingControl.enabled", "false", "A")]
    for sm, q, p, enabled, rc in [("SM1", "5", "-50", "true", "RC1"), ("SM2", "7", "-50", "false", "RC1"),
                                  ("SM3", "9", "-500", "true", "RC1"), ("SM4", "11", "-50", "true", "RC2")]:
        rows += [(sm, "Type", "SynchronousMachine", "A"), (sm, "RotatingMachine.q", q, "A"),
                 (sm, "RotatingMachine.p", p, "A"), (sm, "RegulatingCondEq.controlEnabled", enabled, "A"),
                 (sm, "RegulatingCondEq.RegulatingControl", rc, "A")]
    rows += [("SM3", "RotatingMachine.GeneratingUnit", "GU3", "A"), ("GU3", "Type", "ThermalGeneratingUnit", "A"),
             ("GU3", "GeneratingUnit.minOperatingP", "0", "A"), ("GU3", "GeneratingUnit.maxOperatingP", "100", "A"),
             ("SM4", "SynchronousMachine.InitialReactiveCapabilityCurve", "RCC4", "A"),
             ("RCC4", "Type", "ReactiveCapabilityCurve", "A")]
    for i, x in enumerate(["10", "120", "200"]):
        rows += [(f"CD{i}", "Type", "CurveData", "A"), (f"CD{i}", "CurveData.Curve", "RCC4", "A"),
                 (f"CD{i}", "CurveData.xvalue", x, "A")]
    # tap changers
    for tc, ltc, enabled, step in [("TC1", "true", "true", "5"), ("TC2", "false", "true", "7"),
                                   ("TC3", "true", "true", "3")]:
        rows += [(tc, "Type", "RatioTapChanger", "A"), (tc, "TapChanger.ltcFlag", ltc, "A"),
                 (tc, "TapChanger.controlEnabled", enabled, "A"), (tc, "TapChanger.TapChangerControl", "RC1", "A"),
                 (tc, "TapChanger.step", step, "A")]
    # control area + tie flow
    rows += [("CA1", "Type", "ControlArea", "A"), ("CA1", "ControlArea.netInterchange", "100", "A"),
             ("CA1", "ControlArea.pTolerance", "10", "A"), ("CA1", "IdentifiedObject.energyIdentCodeEic", "10YLV", "A"),
             ("CA1", "IdentifiedObject.name", "LV", "A"),
             ("TF1", "Type", "TieFlow", "A"), ("TF1", "TieFlow.ControlArea", "CA1", "A"),
             ("TF1", "TieFlow.Terminal", "T_EI_A1", "A"), ("TF1", "TieFlow.positiveFlowIn", "true", "A")]
    # equivalent shunt, other injections
    rows += [("ESH1", "Type", "EquivalentShunt", "A"),
             ("ES1", "Type", "EnergySource", "A"), ("ES1", "EnergySource.activePower", "30", "A"),
             ("T_ES1", "Type", "Terminal", "A"), ("T_ES1", "Terminal.ConductingEquipment", "ES1", "A"),
             ("T_ES1", "Terminal.TopologicalNode", "TN_B", "A"), ("T_ES1", "ACDCTerminal.connected", "true", "A"),
             ("ENI1", "Type", "ExternalNetworkInjection", "A"), ("ENI1", "ExternalNetworkInjection.p", "20", "A"),
             ("T_ENI1", "Type", "Terminal", "A"), ("T_ENI1", "Terminal.ConductingEquipment", "ENI1", "A"),
             ("T_ENI1", "Terminal.TopologicalNode", "TN_B", "A"), ("T_ENI1", "ACDCTerminal.connected", "true", "A")]
    return rows


def _rows_sv():
    inst = "SV"
    rows = [("FM_SV", "Type", "FullModel", inst), ("FM_SV", "Model.DependentOn", "FM_OLD", inst)]
    for sv, tn, v, angle in [("SVV1a", "XN1", "0", "0"), ("SVV1b", "XN1", "401", "2"), ("SVV2", "XN2", "0", "0"),
                             ("SVV3", "XN3", "399", "1"), ("SVVA", "TN_A", "110", "0"), ("SVVB", "TN_B", "110", "0")]:
        rows += [(sv, "Type", "SvVoltage", inst), (sv, "SvVoltage.TopologicalNode", tn, inst),
                 (sv, "SvVoltage.v", v, inst), (sv, "SvVoltage.angle", angle, inst)]
    for t, p in [("T_EI_A1", "95"), ("T_EI_B1", "-10"), ("T_EI_A2", "3"), ("T_EI_B2", "-5"), ("T_EI_A3", "4"),
                 ("T_EI_INT", "12"), ("T_ES1", "33"), ("T_ENI1", "25")]:
        f = f"SPF_{t}"
        rows += [(f, "Type", "SvPowerFlow", inst), (f, "SvPowerFlow.Terminal", t, inst),
                 (f, "SvPowerFlow.p", p, inst), (f, "SvPowerFlow.q", "0.5", inst)]
    rows += [("SSC1", "Type", "SvShuntCompensatorSections", inst),
             ("SSC1", "SvShuntCompensatorSections.ShuntCompensator", "ESH1", inst),
             ("SSC1", "SvShuntCompensatorSections.sections", "1", inst),
             ("SSC2", "Type", "SvShuntCompensatorSections", inst),
             ("SSC2", "SvShuntCompensatorSections.ShuntCompensator", "SH_REAL", inst),
             ("SSC2", "SvShuntCompensatorSections.sections", "2", inst)]
    rows += [("ISL_BIG", "Type", "TopologicalIsland", inst)]
    rows += [("ISL_BIG", "TopologicalIsland.TopologicalNodes", f"TN_{i}", inst) for i in range(12)]
    rows += [("ISL_SMALL", "Type", "TopologicalIsland", inst),
             ("ISL_SMALL", "TopologicalIsland.TopologicalNodes", "TN_S1", inst),
             ("ISL_SMALL", "TopologicalIsland.TopologicalNodes", "TN_S2", inst)]
    rows += [("STS1", "Type", "SvTapStep", inst), ("STS1", "SvTapStep.TapChanger", "TC1", inst),
             ("STS1", "SvTapStep.position", "6", inst),
             ("STS2", "Type", "SvTapStep", inst), ("STS2", "SvTapStep.TapChanger", "TC2", inst),
             ("STS2", "SvTapStep.position", "8", inst)]
    return rows


def _rows_ssh():
    inst = "SSH"
    rows = [("FM_SSH", "Type", "FullModel", inst),
            ("FM_SSH", "Model.profile", "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1", inst)]
    for ei, p, q in [("EI_A1", "10", "1"), ("EI_B1", "-10", "-1"), ("EI_A2", "5", "0.5"), ("EI_B2", "-5", "-0.5"),
                     ("EI_A3", "4", "0.4"), ("EI_INT", "10", "2")]:
        rows += [(ei, "Type", "EquivalentInjection", inst), (ei, "EquivalentInjection.p", p, inst),
                 (ei, "EquivalentInjection.q", q, inst), (ei, "EquivalentInjection.regulationStatus", "true", inst)]
    for t, connected in [("T_EI_A1", "true"), ("T_EI_B1", "true"), ("T_EI_A2", "false"), ("T_EI_B2", "true"),
                         ("T_EI_A3", "false"), ("T_EI_INT", "true"), ("T_ES1", "true"), ("T_ENI1", "true")]:
        rows += [(t, "Type", "Terminal", inst), (t, "ACDCTerminal.connected", connected, inst)]
    for sm, q in [("SM1", "15"), ("SM2", "17"), ("SM3", "19"), ("SM4", "21")]:
        rows += [(sm, "Type", "SynchronousMachine", inst), (sm, "RotatingMachine.q", q, inst)]
    for tc, step in [("TC1", "6"), ("TC2", "8"), ("TC3", "3")]:
        rows += [(tc, "Type", "RatioTapChanger", inst), (tc, "TapChanger.step", step, inst)]
    rows += [("CA1", "Type", "ControlArea", inst), ("CA1", "ControlArea.netInterchange", "100", inst),
             ("CA1", "ControlArea.pTolerance", "10", inst),
             ("ES1", "Type", "EnergySource", inst), ("ES1", "EnergySource.activePower", "30", inst),
             ("ENI1", "Type", "ExternalNetworkInjection", inst), ("ENI1", "ExternalNetworkInjection.p", "20", inst)]
    return rows


def build():
    """Return (original_models, cgm_sv, cgm_ssh) as pandas triplet tables."""
    return triplets(_rows_original()), triplets(_rows_sv()), triplets(_rows_ssh())
