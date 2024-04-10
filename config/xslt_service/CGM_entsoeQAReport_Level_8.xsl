<?xml version="1.0" encoding="UTF-8" ?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://entsoe.eu/checks" version="1.0">
    <xsl:output method="xml" omit-xml-declaration="no" encoding="UTF-8" indent="yes" />
    <xsl:template match="/">
    
    <!--ROOT ELEMENT-->
        <xsl:element name = "QAReport">
            <xsl:attribute name="created" select="current-dateTime()"/>
            <xsl:attribute name="schemeVersion">
                <text>2.0</text>
            </xsl:attribute>
            <xsl:attribute name="serviceProvider">
                <text>BALTICRSC</text>
            </xsl:attribute>
            
            <!-- <xsl:variable name="LOADFLOW_CONVERGED" select="/Result/LoadFlowResultSummary[1]/LoadFlowSummary/LoadFlow_Converged='Converged'" /> -->

        	<!--CGM REPORT-->
            <xsl:element name = "CGM">
                        <xsl:attribute name="created">
                            <xsl:value-of select="Result/MergeInformation/MetaData/creationDate"/>
                        </xsl:attribute>
                        <xsl:attribute name="resource">
                            <xsl:value-of select="Result/MergeInformation/MetaData/modelid"/>
                        </xsl:attribute>
                        <xsl:attribute name="qualityIndicator">
                        <text>Plausible</text>

                        </xsl:attribute>
                        <xsl:attribute name="scenarioTime">
                            <xsl:value-of select="Result/MergeInformation/MetaData/scenarioDate"/>
                        </xsl:attribute>
                        <xsl:attribute name="version">
                            <xsl:value-of select="Result/MergeInformation/MetaData/versionNumber"/>
                        </xsl:attribute>
                        <xsl:attribute name="processType">
                            <xsl:value-of select="Result/MergeInformation/MetaData/timeHorizon"/>
                        </xsl:attribute>


                <xsl:for-each select="Result/ModelInformation">


                                    <!--IGM REPORT-->
                                    <xsl:element name = "IGM">
                                        <xsl:attribute name="created">
                                            <xsl:value-of select="MetaData/creationDate"/>
                                        </xsl:attribute>
                                        <xsl:attribute name="processType">
                                            <xsl:value-of select="MetaData/timeHorizon"/>
                                        </xsl:attribute>
                                        <xsl:attribute name="qualityIndicator">
                                            <text>Plausible</text>
                                        </xsl:attribute>
                                        <xsl:attribute name="scenarioTime">
                                            <xsl:value-of select="MetaData/scenarioDate"/>
                                        </xsl:attribute>
                                        <xsl:variable name="MAS" select="MetaData/modelingAuthoritySet" />
                                        <xsl:attribute name="tso">
                                            <xsl:value-of select="MetaData/modelPartReference"/>
                                        </xsl:attribute>
                                        <xsl:attribute name="version">
                                            <xsl:value-of select="MetaData/versionNumber"/>
                                        </xsl:attribute>

                                        <!--REFERENCES-->
                                        <xsl:for-each select = "MetaData/Component">
                                            <xsl:element name = "resource">
                                                <xsl:value-of select="modelid"/>
                                            </xsl:element>
                                        </xsl:for-each>


                        </xsl:element>


                </xsl:for-each>

                <xsl:element name = "EMFInformation">

                            <xsl:attribute name="mergingEntity">
                                <xsl:value-of select="Result/MergeInformation/MetaData/mergingEntity"/>
                            </xsl:attribute>
                            <xsl:attribute name="cgmType">
                                <xsl:value-of select="Result/MergeInformation/MetaData/mergingArea"/>
                            </xsl:attribute>

                    </xsl:element>

                <!--ERRORS-->

                <!-- NOT IMPLEMENTED
                <xsl:variable name="MAS" select="ModelingAuthority/MAS" />
				<xsl:if test="not($LOADFLOW_CONVERGED)">
					<RuleViolation validationLevel="8" ruleId="8_2" severity="WARNING">
						<Message>MSG 53. WARNING: IGM for {MAS}<xsl:value-of select="$MAS"/> has been substituted.</Message>
					</RuleViolation>
				</xsl:if>

                <xsl:if test="not($LOADFLOW_CONVERGED)">
                    <RuleViolation validationLevel="8" ruleId="8_3" severity="WARNING">
    					<Message>MSG 54. WARNING: Calculated voltage at TopologicalNode {IdentifiedObject.name} is too high.</Message>
					</RuleViolation>
				</xsl:if>

                <xsl:if test="not($LOADFLOW_CONVERGED)">
                    <RuleViolation validationLevel="8" ruleId="8_4" severity="WARNING">
    					<Message>MSG 55. WARNING: Calculated voltage at TopologicalNode {IdentifiedObject.name} is too low.</Message>
					</RuleViolation>
				</xsl:if>

                <xsl:if test="not($LOADFLOW_CONVERGED)">
                    <RuleViolation validationLevel="8" ruleId="8_5" severity="WARNING">
						<Message>MSG 56. WARNING: Base case violation detected for monitored element {IdentifiedObject.name}</Message>
					</RuleViolation>
				</xsl:if>

                <xsl:if test="not($LOADFLOW_CONVERGED)">
                	<RuleViolation validationLevel="8" ruleId="8_6" severity="WARNING">
						<Message>MSG 57. WARNING: SvInjection.qInjection bound for SynchronousMachine {IdentifiedObject.name} (low Qlimit has been reached)</Message>
					</RuleViolation>
				</xsl:if>

                <xsl:if test="not($LOADFLOW_CONVERGED)">
            		<RuleViolation validationLevel="8" ruleId="8_7" severity="WARNING">
						<Message>MSG 58. WARNING: SvInjection.qInjection bound for SynchronousMachine {IdentifiedObject.name} (high Qlimit has been reached)</Message>
					</RuleViolation>
				</xsl:if>

                <xsl:if test="not($LOADFLOW_CONVERGED)">
        			<RuleViolation validationLevel="8" ruleId="8_8" severity="WARNING">
						<Message>MSG 59. WARNING: CGM didn’t converge for synchronous area {Synchronous area} with default power flow settings, calculation results may not be realistic.</Message>
					</RuleViolation>
				</xsl:if>


                <xsl:if test="not($LOADFLOW_CONVERGED)">
    				<RuleViolation validationLevel="8" ruleId="8_9" severity="ERROR">
						<Message>MSG 60. ERROR: CGM didn’t converge for synchronous area {Synchronous area} with relaxed power flow settings, calculation results not available.</Message>
					</RuleViolation>
				</xsl:if>
                -->
            
            </xsl:element>
                          
        </xsl:element>
        
    </xsl:template>
</xsl:transform>
